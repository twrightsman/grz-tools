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
from typing import Dict

from collections import OrderedDict

from grz_upload.file_operations import Crypt4GH
from grz_upload.file_operations import calculate_md5, calculate_sha256, read_yaml
from grz_upload.file_validator import FileValidator
from grz_upload.upload import S3UploadWorker

log = logging.getLogger(__name__)


class Worker(object):
    def __init__(self, folder_root):
        # derive folder structure
        self.__submission_root_dir = folder_root
        self.__submission_files_dir = self.__submission_root_dir / "files"
        self.__submission_metadata_dir = self.__submission_root_dir / "metadata"
        self.__submission_encrypted_dir = self.__submission_root_dir / "encrypt"
        self.__submission_log_dir = self.__submission_root_dir / "log"

        self.__metadata_file = self.__submission_metadata_dir / "metadata.json"
        self.__metadata = None

        # Create log folder if non-existent yet
        # Raises error if log folder is not a directory
        self.__submission_log_dir.mkdir(mode=0o770, parents=True, exist_ok=True)

        # derive progress log files
        self.__progress_file_checksum = self.__submission_log_dir / "progress_checksum.yaml"
        self.__progress_file_encrypt = self.__submission_log_dir / "progress_encrypt.yaml"
        self.__progress_file_upload = self.__submission_log_dir / "progress_upload.yaml"

        self.__submission_files_dict = OrderedDict()
        self.__write_progress = False

    # def read_yaml(self, filepath: str) -> Dict:
    #     """
    #     Read a yaml file and store details in a dictionary
    #
    #     :param filepath: path to configuration file
    #     :return: dictionary of s3 configurations parameter
    #     """
    #     temp_dict = {}
    #     with open(str(filepath), "r", encoding="utf-8") as filein:
    #         try:
    #             temp_dict = safe_load(filein)
    #         except YAMLError:
    #             temp_dict = {}
    #             for i in format_exc().split("\n"):
    #                 log.error(i)
    #     return temp_dict

    # def check_yaml(self):
    #     temp = ("s3_url", "s3_access_key", "s3_secret", "s3_bucket")
    #     failed = False
    #     for i in temp:
    #         if i not in self.__s3_dict:
    #             # log.error(f"Please provide {i} in {self.__config_file}")
    #             failed = True
    #     if "use_https" in self.__s3_dict:
    #         if self.__s3_dict["use_https"] and not self.__s3_dict["s3_url"].startswith(
    #                 "https://"
    #         ):
    #             self.__s3_dict["s3_url"] = f'https://{self.__s3_dict["s3_url"]}'
    #     return failed

    def show_summary(self, which: str):
        before, now, failed, finished = 0, 0, 0, 0
        log.info(f"Summary: {which}")
        log.info(f"Total number of files: {len(self.__submission_files_dict)}")
        for filepath in self.__submission_files_dict:
            if self.__submission_files_dict[filepath]["checked"]:
                before += 1
            else:
                now += 1
            if self.__submission_files_dict[filepath]["status"] == "Finished":
                finished += 1
            elif self.__submission_files_dict[filepath]["status"] == "Failed":
                failed += 1

        log.info(f"Total number of files checked before current process: {before}")
        log.info(f"Total number of files checked in current process: {now}")
        log.info(f"Total number of failed files: {failed}")
        log.info(f"Total number of finished files: {finished}")
        if len(self.__submission_files_dict) == finished:
            log.info(f"Summary: {which} - Process Complete")
        else:
            log.info(f"Summary: {which} - Process Incomplete")
            log.warning(f"Please tend to any errors before you continue")

    def _get_dictionary(self):
        return {'total': None, 'found': None, 'checked': False, 'status': "in progress", 'checksum': None}

    '''
    Method checks if the file exists
    @rtype: boolean
    @return: boolean
    '''

    # TODO: can go to file_operations
    def check_metadata_file(self) -> bool:
        if not self.__metadata_file.is_file():
            log.error(f"Please provide a valid path to the metadata file: {self.__metadata_file}")
            return False
        return True

    '''
    Method reads in a json file.
    @param filepath: Path
    @rtype: tuple
    @return: tuple (boolean, dict)
    '''

    # TODO: can go to file_operations
    def load_json(self, filepath: Path) -> tuple:
        try:
            with open(filepath, "r", encoding="utf-8") as jsonfile:
                json_dict = json.load(jsonfile)
        except json.JSONDecodeError:
            log.error(f"The provided file is not a valid JSON: {filepath}")
            return False, None
        return True, json_dict

    def parse_json(self):
        valid = True
        for donor in self.__metadata.get("Donors", {}):
            for lab_data in donor.get("LabData", {}):
                for sequence_data in lab_data.get("SequenceData", {}):
                    for files_data in sequence_data.get("files", {}):
                        filename = files_data["filepath"]
                        filepath = self.__submission_root_dir / "files" / filename
                        # check if the file is already in the dictionary, add it to dictionary
                        if filepath in self.__submission_files_dict:
                            log.error(f"The filename appears more than once in the metadata file: {filename}")
                            log.error("Files having more than a single entries are not permitted!")
                            valid = False
                        else:
                            self.__submission_files_dict[filepath] = self._get_dictionary()
                            self.__submission_files_dict[filepath]['total'] = True
                        # check if the file exists and add status
                        if filepath.is_file():
                            self.__submission_files_dict[filepath]['found'] = True
                        else:
                            self.__submission_files_dict[filepath]['found'] = False
                            log.error(f"The file: {filename} does not exist in {self.__submission_files_dir}")
                            valid = False
                        self.__submission_files_dict[filepath]['checksum'] = files_data["fileChecksum"]
        return valid

    def get_dict_for_report(self):
        temp = {str(i): self.__submission_files_dict[i]['status'] for i in self.__submission_files_dict}
        return temp

    def validate_checksum(self):
        log.info('Starting validation of sha256 checksums of sequencing data')
        if not self.check_metadata_file(): exit(2)
        valid, self.__metadata = self.load_json(self.__metadata_file)
        if not valid: exit(2)
        if not self.parse_json(): exit(2)
        self.__write_progress = True
        if not self.__progress_file_checksum.is_file():
            log.info('Progress report for checksum validation does not exist: Start fresh')
            progress_dict = {}
        else:
            log.info('Progress report for checksum validation exist: Picking up where we left')
            progress_dict = read_yaml(self.__progress_file_checksum)
            progress_dict = {} if progress_dict is None else {Path(i[0]): i[1] for i in progress_dict.items()}

        for filepath in self.__submission_files_dict:
            if filepath in progress_dict and progress_dict[filepath] == 'Finished':
                log.info(f"Skip file: {filepath.name}, has been checked before - Skipping")
                self.__submission_files_dict[filepath]["status"] = "Finished"
                self.__submission_files_dict[filepath]["checked"] = True
            else:
                checksum_calc = calculate_sha256(filepath)
                if checksum_calc != self.__submission_files_dict[filepath]['checksum']:
                    log.error(
                        f"Provided checksum of file: {filepath.name} does not match calculated checksum: {self.__submission_files_dict[filepath]['checksum']} (metadata) != {checksum_calc} (calculated)")
                    self.__submission_files_dict[filepath]["status"] = "Failed"
                else:
                    log.info(f"Provided checksum of file: {filepath.name} matches calculated checksum")
                    self.__submission_files_dict[filepath]["status"] = "Finished"
        log.info('Finished validation of sha256 checksums of sequencing data')

    def checksum_validation(self):
        """
        Check the validity of a JSON file and process its contents.

        :raises json.JSONDecodeError: If the provided file is not a valid JSON.
        :raises FileNotFoundError: If a file specified in the JSON does not exist.
        :raises ValueError: If the md5sum of a file does not match the provided checksum.
        """
        self.load_json()
        stop = False
        for donor in self.__json_dict.get("Donors", {}):
            for lab_data in donor.get("LabData", {}):
                for sequence_data in lab_data.get("SequenceData", {}):
                    for files_data in sequence_data.get("files", {}):
                        self.__file_total += 1
                        filename = files_data["filepath"]
                        log.info(f"Checksum validation: {filename}")
                        filechecksum = files_data["fileChecksum"]

                        is_valid = self.file_validator.validate_file(filename, filechecksum)

                        if not is_valid:
                            self.__file_invalid += 1
                            stop = True
                        else:
                            self.__file_todo += 1

                        log.info(f"Preprocessing: {filename} - done")

        if stop:
            log.error(
                "Please tend to the errors listed above and restart the script afterwards!"
            )
            exit(2)

        if self.__file_todo == 0:
            log.info(self.__metadata)
            log.warning("All files in the metafile have been already processed")
            exit(1)

    def encrypt(self):
        """
        Prepare the submission for upload to GRZ S3.

        :param encrypt: Boolean flag to encrypt files before upload.
        :return: String indicating the status of the submission preparation.
        """

        log.info("Preparing encryption...")

        # Step 2: Prepare S3 worker and get encryption key

        # s3_worker = S3UploadWorker(self.__s3_dict, self.__pubkey)
        try:
            public_keys = Crypt4GH.prepare_c4gh_keys(self.__pubkey)
            log.info("Public keys retrieved successfully.")
        except Exception as e:
            log.error("Failed to prepare public keys: %s", e)
            return "Public key retrieval failed"

        # Step 3: Encrypt files
        try:
            self.load_json()
            log.info("Starting parse json")
            for donor in self.__json_dict.get("Donors", {}):
                for lab_data in donor.get("LabData", {}):
                    for sequence_data in lab_data.get("SequenceData", {}):
                        for files_data in sequence_data.get("files", {}):
                            filename = files_data["filepath"]
                            fullpath = self.__submission_root_dir / "files" / filename
                            if not fullpath.is_file(): log.error(f"File does not exist: {fullpath}")
                            output_file_path = fullpath.parent / (fullpath.name + ".c4gh")
                            original_sha256 = calculate_sha256(fullpath)
                            log.info("Encrypting file: %s", filename)
                            Crypt4GH.encrypt_file(fullpath, output_file_path, public_keys)
                            encrypted_sha256 = calculate_sha256(output_file_path)
                            log.info("Encryption successful for file: %s", filename)
                            log.info("Original SHA256: %s", original_sha256)
                            log.info("Encrypted SHA256: %s", encrypted_sha256)
        except Exception as e:
            log.error("Encryption failed for one or more files: %s", e)
            return "Encryption failed"

        log.info("Encryption completed successfully.")
        return "Encryption preparation successful"

    def get_log(self):
        return log
    def build_write_s3cfg(self):
        temp = ['[default]']
        temp.append(f'access_key = {self.__s3_dict["s3_access_key"]}')
        temp.append(f'secret_key = {self.__s3_dict["s3_secret"]}')
        temp.append(f'host_base = {self.__s3_dict["s3_url"]}')
        temp.append(f'host_bucket = {self.__s3_dict["host_bucket"]}')
        self.__use_s3file = Path('/tmp') / 's3cfg'
        with open(self.__use_s3file, 'w') as fileout:
            fileout.write('\n'.join(temp) + '\n')

    def upload(self):
        part_upload = True
        log.info("Preparing upload...")
        s3_worker = S3UploadWorker(self.__s3_dict, self.__pubkey)
        # Step 3: Encrypt files
        try:
            self.load_json()
            log.info("Starting parse json")
            for donor in self.__json_dict.get("Donors", {}):
                for lab_data in donor.get("LabData", {}):
                    for sequence_data in lab_data.get("SequenceData", {}):
                        for files_data in sequence_data.get("files", {}):
                            filename = files_data["filepath"]
                            fullpath = self.__folderpath / "files" / filename
                            if not fullpath.is_file:
                                log.error(f'File does not exist: {fullpath}')
                                continue
                            output_file_path = fullpath.parent / (fullpath.name + ".c4gh")
                            if not output_file_path.is_file:
                                log.error(f'File does not exist: {output_file_path}')
                                continue
                            if self.__use_s3cmd:
                                self.build_write_s3cfg()
                                if part_upload:
                                    s3_worker._upload_multipart_s3cmd(str(self.__use_s3file),
                                                                      self.__s3_dict['s3_bucket'],
                                                                      str(output_file_path), 50)
                                else:
                                    s3_worker._upload_s3cmd(str(self.__use_s3file), self.__s3_dict['s3_bucket'],
                                                            str(output_file_path))
                                self.__use_s3file.unlist()
                            else:
                                if part_upload:
                                    s3_worker._multipart_upload(output_file_path, output_file_path.name)
                                else:
                                    s3_worker._upload(output_file_path, output_file_path.name)

        except Exception as e:
            log.error("Upload failed for one or more files: %s", e)
            return "Upload Failed"

        log.info("Upload completed successfully.")
        return "Upload Complete"

    def main(self):
        if not self.__config_file.is_file():
            log.error(
                "Please provide a valid path to the config file (-c/--config option)"
            )
            exit(2)
        self.__s3_dict = self.read_yaml(self.__config_file)
        if self.check_yaml():
            exit(2)

        if not self.__pubkey.is_file():
            log.error(
                "Please provide a valid path to the public cryp4gh key (--pubkey_grz)"
            )
            exit(2)
        # self.check_public_key()

    def get_progress_file_checksum(self):
        return self.__progress_file_checksum

    def get_write_progress(self):
        return self.__write_progress

    log = property(get_log)
    progress_file_checksum = property(get_progress_file_checksum)
    write_progress = property(get_write_progress)

    def get_meta_file(self):
        return self.__meta_file

    def get_s3_dict(self):
        return self.__s3_dict

    def get_pubkey_grz(self):
        return self.__pubkey

    def get_s3_cmd(self):
        return self.__use_s3cmd

    s3_dict = property(get_s3_dict)
    json_dict = property(get_json_dict)
    json_file = property(get_json_file)
    meta_dict = property(get_meta_dict)
    meta_file = property(get_meta_file)
    pubkey_grz = property(get_pubkey_grz)
    s3_cmd = property(get_s3_cmd)
