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

from grz_upload.file_operations import Crypt4GH
from grz_upload.file_operations import calculate_md5, calculate_sha256
from grz_upload.file_validator import FileValidator
from grz_upload.upload import S3UploadWorker

log = logging.getLogger(__name__)


class Parser(object):
    def __init__(self, folderpath, s3_config=None):
        self.__parser = ArgumentParser(
            description="""
        Manages the encryption and upload of files into the s3 structure of a GRZ.
        """,
            formatter_class=RawDescriptionHelpFormatter,
        )
        self.__folderpath = ""
        self.__s3_dict = {}
        self.__meta_file = None
        self.__json_file = None
        self.__meta_dict = {}
        self.__json_dict = {}
        self.__pubkey = ""
        self.__file_done = 0
        self.__file_todo = 0
        self.__file_failed = 0
        self.__file_total = 0
        self.__file_invalid = 0
        self.file_validator = FileValidator(folderpath)
        self.__use_s3cmd = False
        self.__use_s3file = ''
        
        if s3_config:
            self.__config_file = s3_config
        else:
            self.__config_file = None


    def set_options(self, options, pubkey=True, s3_config=True):
        self.__folderpath = Path(options["folderpath"]).expanduser()
        self.check_metadata_file()
        if pubkey:
            self.__pubkey = Path(options["public_key"]).expanduser()
        if s3_config:
            s3_config_file = Path(options["config_file"]).expanduser()
            self.__s3_dict = self.read_yaml(s3_config_file)
            if self.check_yaml(): exit(2)
            self.__use_s3cmd = options["use_s3cmd"]

    def check_metadata_file(self):
        metadata_file = self.__folderpath / "metadata" / "metadata.json"
        if not metadata_file.is_file():
            log.error(
                f"Please provide a valid path to the metadata file: {metadata_file}"
            )
            exit(2)
        self.__json_file = metadata_file
        

    """
    Read a yaml file and store details in a dictionary
    @param filepath: String
    @rtype: dictionary
    @return: dictionary of s3 configurations parameter
    """

    def read_yaml(self, filepath):
        temp_dict = {}
        with open(str(filepath), "r", encoding="utf-8") as filein:
            try:
                temp_dict = safe_load(filein)
            except YAMLError:
                temp_dict = {}
                for i in format_exc().split("\n"):
                    log.error(i)
        return temp_dict

    def check_yaml(self):
        temp = ("s3_url", "s3_access_key", "s3_secret", "s3_bucket")
        failed = False
        for i in temp:
            if i not in self.__s3_dict:
                log.error(f"Please provide {i} in {self.__config_file}")
                failed = True
        
        if "use_https" in self.__s3_dict:
            if self.__s3_dict["use_https"] and not self.__s3_dict["s3_url"].startswith("https://"):
                self.__s3_dict["s3_url"] = f'https://{self.__s3_dict["s3_url"]}'
        return failed
    
    def load_json(self):
        try:
            with open(str(self.__json_file), "r", encoding="utf-8") as jsonfile:
                self.__json_dict = json.load(jsonfile)
        except json.JSONDecodeError:
            log.error(
                f"The provided file at {self.__json_file} is not a valid JSON (option --metafile)."
            )
            exit()

    def checksum_validation(self):
        """
        Check the validity of a JSON file and process its contents.

        Raises:
            json.JSONDecodeError: If the provided file is not a valid JSON.
            FileNotFoundError: If a file specified in the JSON does not exist.
            ValueError: If the md5sum of a file does not match the provided checksum.

        Returns:
            None
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
            log.info(self.__meta_dict)
            log.warning("All files in the metafile have been already processed")
            exit(1)

    def get_metainfo_file(self, filename):
        return self.__meta_dict[filename]

    def write_json(self):
        log.info(f"Meta information written to: {self.__meta_file}")
        with open(self.__meta_file, "w") as file:
            json.dump(self.__json_dict, file, indent=4)

    def show_information(self, logfile):
        """
        Display information about the current state of the parser.

        Parameters:
        - logfile (str): The path to the log file.

        Returns:
        None
        """
        log.info("s3 config file %s", self.__config_file)
        # log.info(f'meta file: {self.__meta_file}')
        #log.info("meta file: %s", self.__json_file)
        log.info("GRZ public crypt4gh key: %s", self.__pubkey)
        #log.info("log file: %s", logfile)
        log.info("total files in metafile: %s", self.__file_total)
        log.info("uploaded files: %s", self.__file_done)
        log.info("failed files: %s", self.__file_failed)
        log.info("invalid files: %s", self.__file_invalid)
        log.info("waiting files: %s", self.__file_todo)

    def encrypt(self):
        """
        Prepare the submission for upload to GRZ S3.

        :param encrypt: Boolean flag to encrypt files before upload.
        :return: String indicating the status of the submission preparation.
        """

        log.info("Preparing encryption...")

        # Step 2: Prepare S3 worker and get encryption key

        #s3_worker = S3UploadWorker(self.__s3_dict, self.__pubkey)
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
                            fullpath = self.__folderpath / "files" / filename
                            output_file_path = fullpath.with_suffix(".c4gh")
                            log.info("Encrypting file: %s", filename)
                            original_sha256, encrypted_sha256 = Crypt4GH.encrypt_file(
                                fullpath, output_file_path, public_keys
                            )

                            log.info("Encryption successful for file: %s", filename)
                            log.info("Original SHA256: %s", original_sha256)
                            log.info("Encrypted SHA256: %s", encrypted_sha256)
        except Exception as e:
            log.error("Encryption failed for one or more files: %s", e)
            return "Encryption failed"

        log.info("Encryption completed successfully.")
        return "Encryption preparation successful"

    def build_write_s3cfg(self):
        temp = ['[default]']
        temp.append(f'access_key = {self.__s3_dict["s3_access_key"]}')
        temp.append(f'secret_key = {self.__s3_dict["s3_secret"]}')
        temp.append(f'host_base = {self.__s3_dict["s3_url"]}')
        temp.append(f'host_bucket = {self.__s3_dict["host_bucket"]}')
        self.__use_s3file = Path('/tmp') / 's3cfg'
        with open(self.__use_s3file, 'w') as fileout:
            fileout.write('\n'.join(temp)+'\n')
        

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
                                    s3_worker._upload_multipart_s3cmd(str(self.__use_s3file), self.__s3_dict['s3_bucket'], str(output_file_path), 50)
                                else:
                                    s3_worker._upload_s3cmd(str(self.__use_s3file), self.__s3_dict['s3_bucket'], str(output_file_path))
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
        #self.check_public_key()

        if not self.__json_file.is_file():
            log.error("Please provide a valid path to the meta file (--metafile)")
            exit(2)
        #self.check_json()

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

    def get_s3_cmd(self):
        return self.__use_s3cmd

    s3_dict = property(get_s3_dict)
    json_dict = property(get_json_dict)
    json_file = property(get_json_file)
    meta_dict = property(get_meta_dict)
    meta_file = property(get_meta_file)
    pubkey_grz = property(get_pubkey_grz)
    s3_cmd = property(get_s3_cmd)
