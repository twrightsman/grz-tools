import json
import shutil
import logging
import logging.config
from argparse import ArgumentParser as ArgumentParser
from argparse import RawDescriptionHelpFormatter
from base64 import b64decode
from os import environ
from pathlib import Path
from traceback import format_exc

from yaml import safe_load, YAMLError

from grz_upload.file_manager import FileManager
from grz_upload.file_operations import Crypt4GH
from grz_upload.file_operations import calculate_md5
from grz_upload.upload import S3UploadWorker

log = logging.getLogger(__name__)

class Parser(object):
    def __init__(self):
        self.__parser = ArgumentParser(description="""
        Manages the encryption and upload of files into the s3 structure of a GRZ.
        """, formatter_class=RawDescriptionHelpFormatter)
        self.__config_file = ''
        self.__s3_dict = {}
        self.__meta_file = None
        self.__json_file = None
        self.__meta_dict = {}
        self.__json_dict = {}
        self.__pubkey = ''
        self.__file_done = 0
        self.__file_todo = 0
        self.__file_failed = 0
        self.__file_total = 0
        self.__file_invalid = 0
        self.file_manager = FileManager()

    def set_options(self, options):
        self.__config_file = self.get_absolute_path_pathlib(options["config_file"])
        self.__json_file = self.get_absolute_path_pathlib(options["meta_file"])
        self.__pubkey = self.get_absolute_path_pathlib(options["public_key"])

    '''
    Method changes the inputstring to an absolute path
    @param filepath: String
    @rtype: Path
    @return: Path
    '''

    def get_absolute_path_pathlib(self, filepath):
        if filepath == '~':
            filepath = environ['HOME']
        elif filepath[0] == '~':
            filepath = filepath.replace(filepath[0], environ['HOME'])
        else:
            filepath = Path(filepath).resolve()
        return Path(filepath)

    '''
    Read a yaml file and store details in a dictionary
    @param filepath: String
    @rtype: dictionary
    @return: dictionary of s3 configurations parameter
    '''

    def read_yaml(self, filepath):
        temp_dict = {}
        with open(str(filepath), 'r', encoding='utf-8') as filein:
            try:
                temp_dict = safe_load(filein)
            except YAMLError:
                temp_dict = {}
                for i in format_exc().split('\n'): log.error(i)
        return temp_dict

    def check_yaml(self):
        temp = ('s3_url', 's3_access_key', 's3_secret', 's3_bucket')
        failed = False
        for i in temp:
            if i not in self.__s3_dict:
                log.error(f'Please provide {i} in {self.__config_file}')
                failed = True
        if 'use_https' in self.__s3_dict:
            if self.__s3_dict['use_https'] and not self.__s3_dict['s3_url'].startswith('https://'): self.__s3_dict[
                's3_url'] = f'https://{self.__s3_dict["s3_url"]}'
        return failed

    def check_public_key(self):
        with open(self.__pubkey, 'rb') as f:
            f.readline()
            key_data = f.readline().strip()
            f.readline()
        try:
            key_data = b64decode(key_data)
        except Exception as e:
            log.error('Public crypt4gh key file of GRZ is not a valid file (--pubkey_grz option)')
            log.error(f"Key decoding error: {e}")
            exit(2)
        # Check if the key is 32 bytes long (Curve25519 public key size)
        if len(key_data) != 32:
            log.error('Public crypt4gh key file of GRZ is not a valid file (--pubkey_grz option)')
            log.error('Client public key must be a 32 bytes long bytes sequence')
            exit(2)

    def build_dict(self, filename, fullpath, checksum, checksum_enc, upload_status):
        temp = {}
        temp['file_id'] = filename
        temp['file_location'] = fullpath
        temp['original_md5'] = checksum
        temp['encrypted_md5'] = checksum_enc
        temp['upload_status'] = upload_status
        temp['filename_encrypted'] = filename if filename.endswith(S3UploadWorker.EXT) else filename + S3UploadWorker.EXT
        return temp

    def check_json(self):
        try:
            with open(str(self.__json_file), 'r', encoding='utf-8') as jsonfile:
                self.__json_dict = json.load(jsonfile)
        except json.JSONDecodeError:
            log.error(
                f'The provided file at {self.__json_file} is not a valid JSON (option --metafile).')
            exit()
        filepaths = []
        stop = False
        for donor in self.__json_dict.get("Donors", {}):
            for lab_data in donor.get("LabData", {}):
                for sequence_data in lab_data.get("SequenceData", {}):
                    for files_data in sequence_data.get("files", {}):
                        files_invalid = self.__file_invalid
                        self.__file_total += 1
                        filename = files_data['filename']
                        filepath = files_data['filepath']
                        filechecksum = files_data['fileChecksum']
                        filechecksum_enc = files_data.get('fileChecksum_encrypted', '')
                        upload_status = files_data.get('upload_status', 'in progress')
                        if upload_status == 'failed': self.__file_failed += 1
                        fullpath = Path(filepath) / filename
                        if upload_status == 'finished':
                            log.warning(f'File ID {filename} already scanned in metafile - Skipping')
                            self.__file_done += 1
                            self.__meta_dict[filename] = self.build_dict(filename, fullpath, filechecksum,
                                                                         filechecksum_enc, upload_status)
                            continue
                        log.info(f'Preprocessing: {fullpath}')
                        if not fullpath.is_file():
                            log.error(
                                f'The provided file {filename} in {self.__json_file} does not exist.')
                            self.__file_invalid += 1
                            stop = True
                        filechecksum_calc = calculate_md5(fullpath)
                        if filechecksum == filechecksum_calc:
                            log.info(f'Preprocessing: {fullpath} - md5sum correct')
                        else:
                            log.error(
                                f'Preprocessing: {fullpath} - md5sum incorrect - provided: {filechecksum} - calculated: {filechecksum_calc}')
                            if self.__file_invalid == files_invalid: self.__file_invalid += 1
                            stop = True
                        self.__meta_dict[filename] = self.build_dict(filename, fullpath, filechecksum, filechecksum_enc,
                                                                     upload_status)
                        log.info(f'Preprocessing: {fullpath} - done')
                        filepaths.append(str(fullpath))
                        self.__file_todo += 1

        if stop:
            log.error('Please tend to the errors listed above and restart the script afterwards!')
            exit(2)

        if self.__file_todo == 0:
            log.info(self.__meta_dict)
            log.warning('All files in the metafile have been already processed')
            exit(1)

    def get_metainfo_file(self, filename):
        return self.__meta_dict[filename]

    def update_json(self, json_file_path, encrypted_md5):
        CSV_HEADER = ['file_id', 'file_location', 'original_md5', 'filename_encrypted', 'encrypted_md5', 'upload_status']
        count = 0
        for donor in self.__json_dict.get("Donors", {}):
            for lab_data in donor.get("LabData", {}):
                for sequence_data in lab_data.get("SequenceData", {}):
                    for files_data in sequence_data.get("files", {}):
                        filename = files_data['filename']
                        meta_dict = self.get_metainfo_file(filename)                        
                        files_data[CSV_HEADER[3]] = meta_dict[CSV_HEADER[3]]
                        files_data[CSV_HEADER[5]] = meta_dict[CSV_HEADER[5]]
                        files_data['fileChecksum_encrypted'] = encrypted_md5[count]
                        count += 1

        try:
            with open(json_file_path, 'w') as json_file:
                json.dump(self.__json_dict, json_file, indent=4)
            log.info("JSON file updated successfully: %s", json_file_path)
        except Exception as e:
            log.error("Failed to update JSON file: %s", e)

    def write_json(self):
        log.info(f'Meta information written to: {self.__meta_file}')
        with open(self.__meta_file, 'w') as file:
            json.dump(self.__json_dict, file, indent=4)


    def show_information(self, logfile):
        log.info(f's3 config file {self.__config_file}')
        # log.info(f'meta file: {self.__meta_file}')
        log.info(f'meta file: {self.__json_file}')
        log.info(f'GRZ public crypt4gh key: {self.__pubkey}')
        log.info(f'log file: {logfile}')
        log.info(f'total files in metafile: {self.__file_total}')
        log.info(f'uploaded files: {self.__file_done}')
        log.info(f'failed files: {self.__file_failed}')
        log.info(f'invalid files: {self.__file_invalid}')
        log.info(f'waiting files: {self.__file_todo}')

    def prepare_submission(self, encrypt=True):
        """ 
        Prepare the submission for upload to GRZ S3.
        
        :param encrypt: Boolean flag to encrypt files before upload.
        :return: String indicating the status of the submission preparation.
        """

        log.info("Preparing submission...")

        # Step 1: Prepare directory and metadata file
        try:
            files_dir, metadata_file = self.file_manager.prepare_directory()
            log.info("Files directory prepared at: %s", files_dir)
        except Exception as e:
            log.error("Failed to prepare directory: %s", e)
            return "Directory preparation failed"

        # Step 2: Update file directory
        try:
            file_paths = self.file_manager.update_file_directory(self.__json_dict, files_dir)
            log.info("File paths updated")
        except Exception as e:
            log.error("Failed to update file directory: %s", e)
            return "File directory update failed"

        # Step 3: Move metadata
        try:
            self.file_manager.move_metadata(self.__json_dict, metadata_file)
            log.info("Metadata moved to: %s", metadata_file)
        except Exception as e:
            log.error("Failed to move metadata: %s", e)
            return "Metadata move failed"

        # Step 4: Move files
        try:
            new_file_paths = self.file_manager.move_files(file_paths, files_dir)

            file_names = [Path(file).name for file in new_file_paths]
            log.info("Files moved: %s", file_names)
        except Exception as e:
            log.error("Failed to move files: %s", e)
            return "File move failed"

        if encrypt:
            # Step 5: Prepare S3 worker and get encryption key
            s3_worker = S3UploadWorker(self.__s3_dict, self.__pubkey)
            try:
                public_keys = s3_worker.get_encryption_key()
                log.info("Public keys retrieved successfully.")
            except Exception as e:
                log.error("Failed to prepare public keys: %s", e)
                return "Public key retrieval failed"

            # Step 6: Encrypt files
            try:
                encrypted_md5s = []
                for file in new_file_paths:
                    output_file_path = Path(file).with_suffix('.c4gh')
                    original_md5, encrypted_md5 = Crypt4GH.encrypt_file(file, output_file_path, public_keys)
                    file_name = Path(file).name
                    encrypted_md5s.append(encrypted_md5)
                    log.info("Encryption successful for file: %s", file_name)
                    log.info("Original MD5: %s", original_md5)
                    log.info("Encrypted MD5: %s", encrypted_md5)
            except Exception as e:
                log.error("Encryption failed for one or more files: %s", e)
                return "Encryption failed"
        
        self.update_json(metadata_file, encrypted_md5s)

        log.info("Submission preparation completed successfully.")
        return "Submission preparation successful"



        
    def main(self):
        if not self.__config_file.is_file():
            log.error('Please provide a valid path to the config file (-c/--config option)')
            exit(2)
        self.__s3_dict = self.read_yaml(self.__config_file)
        if self.check_yaml(): exit(2)

        if not self.__pubkey.is_file():
            log.error('Please provide a valid path to the public cryp4gh key (--pubkey_grz)')
            exit(2)
        self.check_public_key()

        if not self.__json_file.is_file():
            log.error('Please provide a valid path to the meta file (--metafile)')
            exit(2)
        self.check_json()

    def get_json_dict(self):
        return self.__json_dict

    def get_json_file(self):
        return self.__json_file

    def get_meta_dict(self):
        return self.__meta_dict

    def get_meta_file(self):
        return self.__meta_file

    def get_s3_dict(self):
        return self.__s3_dict

    def get_pubkey_grz(self):
        return self.__pubkey

    s3_dict = property(get_s3_dict)
    json_dict = property(get_json_dict)
    json_file = property(get_json_file)
    meta_dict = property(get_meta_dict)
    meta_file = property(get_meta_file)
    pubkey_grz = property(get_pubkey_grz)