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

log = logging.getLogger("Worker")


class Worker(object):
    def __init__(self, folder_path):
        self.__folderpath = folder_path
        self.__folder_files = self.__folderpath / "files"
        self.__folder_meta = self.__folderpath / "metadata"
        self.__folder_encrypt = self.__folderpath / "encrypt"
        self.__folder_log = self.__folderpath / "log"
        
        self.__metadata_file = self.__folder_meta / "metadata.json"
        self.__meta_dict = None
        
        self.__log = None # own logging instance

        self._create_directory(self.__folder_log, "normal")
        self.__progress_file_checksum = self.__folder_log / "progress_checksum.yaml"
        self.__progress_file_encrypt = self.__folder_log / "progress_encrypt.yaml"
        self.__progress_file_upload = self.__folder_log / "progress_upload.yaml"
        
        self.__files_dict = OrderedDict()
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

    def check_yaml(self):
        temp = ("s3_url", "s3_access_key", "s3_secret", "s3_bucket")
        failed = False
        for i in temp:
            if i not in self.__s3_dict:
                # log.error(f"Please provide {i} in {self.__config_file}")
                failed = True
        if "use_https" in self.__s3_dict:
            if self.__s3_dict["use_https"] and not self.__s3_dict["s3_url"].startswith(
                    "https://"
            ):
                self.__s3_dict["s3_url"] = f'https://{self.__s3_dict["s3_url"]}'
        return failed

    def show_summary(self, which: str):
        before, now, failed, finished = 0, 0, 0, 0
        self.__log.info(f"Summary: {which}")
        self.__log.info(f"Total number of files: {len(self.__files_dict)}")
        for filepath in self.__files_dict:
            if self.__files_dict[filepath]["checked"]:
                before += 1
            else:
                now += 1
            if self.__files_dict[filepath]["status"] == "Finished":
                finished += 1
            elif self.__files_dict[filepath]["status"] == "Failed":
                failed += 1

        self.__log.info(f"Total number of files checked before current process: {before}")
        self.__log.info(f"Total number of files checked in current process: {now}")
        self.__log.info(f"Total number of failed files: {failed}")
        self.__log.info(f"Total number of finished files: {finished}")
        if len(self.__files_dict) == finished:
            self.__log.info(f"Summary: {which} - Process Complete")
        else:
            self.__log.info(f"Summary: {which} - Process Incomplete")
            self.__log.warning(f"Please tend to any errors before you continue")

    def _get_dictionary(self):
        return { 'total' : None, 'found' : None, 'checked' : False, 'status' : "in progress", 'checksum' : None}

    '''
    function creates a directory and throws exception if the directory exists
    or it cannot create the directory
    @param dirpath: Path
    @param typus: string
    @rtype: boolean
    @return: boolean
    '''
    def _create_directory(self, dirpath : Path, typus : str) -> bool:
        try:
            if typus == 'normal': dirpath.mkdir(mode = 0o770, parents = True)
            elif typus == 'other_exe' : dirpath.mkdir(mode = 0o771, parents = True)
            elif typus == 'user_only': dirpath.mkdir(mode = 0o700, parents = True)
            log.warning(f"Directory created: {dirpath}")
        except FileExistsError:
            log.info(f"Directory exists: {dirpath}")
        except OSError:
            log.error(f"Directory not created created: {dirpath}")
            return False
        return True

    '''
    Method checks if the file exists
    @rtype: boolean
    @return: boolean
    '''
    # TODO: can go to file_operations
    def check_metadata_file(self) -> bool:
        if not self.__metadata_file.is_file(): 
            self.__log.error(f"Please provide a valid path to the metadata file: {self.__metadata_file}")
            return False
        return True

    '''
    Method reads in a json file.
    @param filepath: Path
    @rtype: tuple
    @return: tuple (boolean, dict)
    '''
    # TODO: can go to file_operations
    def load_json(self, filepath : Path) -> tuple:
        try:
            with open(filepath, "r", encoding="utf-8") as jsonfile:
                json_dict = json.load(jsonfile)
        except json.JSONDecodeError:
            log.error(f"The provided file is not a valid JSON: {filepath}")
            return False, None
        return True, json_dict

    def parse_json(self):
        valid = True
        for donor in self.__meta_dict.get("Donors", {}):
            for lab_data in donor.get("LabData", {}):
                for sequence_data in lab_data.get("SequenceData", {}):
                    for files_data in sequence_data.get("files", {}):
                        filename = files_data["filepath"]
                        filepath = self.__folderpath / "files" / filename
                        # check if the file is already in the dictionary, add it to dictionary
                        if filepath in self.__files_dict:
                            self.__log.error(f"The filename appears more than once in the metadata file: {filename}")
                            self.__log.error("Files having more than a single entries are not permitted!")
                            valid = False
                        else:
                            self.__files_dict[filepath] = self._get_dictionary()
                            self.__files_dict[filepath]['total'] = True
                        # check if the file exists and add status
                        if filepath.is_file():
                            self.__files_dict[filepath]['found'] = True
                        else:
                            self.__files_dict[filepath]['found'] = False
                            self.__log.error(f"The file: {filename} does not exist in {self.__folder_files}")
                            valid = False
                        self.__files_dict[filepath]['checksum'] = files_data["fileChecksum"]
        return valid
    
    def get_dict_for_report(self):
        temp = {str(i) : self.__files_dict[i]['status'] for i in self.__files_dict}
        return temp

    def validate_checksum(self):
        self.__log = logging.getLogger("Worker.validate_checksum")
        self.__log.info('Starting validation of sha256 checksums of sequencing data')
        if not self.check_metadata_file(): exit(2)
        valid, self.__meta_dict = self.load_json(self.__metadata_file)
        if not valid: exit(2)
        if not self.parse_json(): exit(2)
        self.__write_progress = True
        if not self.__progress_file_checksum.is_file():
            self.__log.info('Progress report for checksum validation does not exist: Start fresh')
            progress_dict = {}
        else:
            self.__log.info('Progress report for checksum validation exist: Picking up where we left')
            progress_dict = read_yaml(self.__progress_file_checksum)
            progress_dict = {} if progress_dict is None else {Path(i[0]) : i[1] for i in progress_dict.items()}

        for filepath in self.__files_dict:
            if filepath in progress_dict and progress_dict[filepath] == 'Finished':
                self.__log.info(f"Skip file: {filepath.name}, has been checked before - Skipping")
                self.__files_dict[filepath]["status"] = "Finished"
                self.__files_dict[filepath]["checked"] = True
            else:
                checksum_calc = calculate_sha256(filepath)
                if checksum_calc != self.__files_dict[filepath]['checksum']:
                    log.error(f"Provided checksum of file: {filepath.name} does not match calculated checksum: {self.__files_dict[filepath]['checksum']} (metadata) != {checksum_calc} (calculated)")
                    self.__files_dict[filepath]["status"] = "Failed"
                else:
                    log.info(f"Provided checksum of file: {filepath.name} matches calculated checksum")
                    self.__files_dict[filepath]["status"] = "Finished"
        self.__log.info('Finished validation of sha256 checksums of sequencing data')

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
            log.info(self.__meta_dict)
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
                            fullpath = self.__folderpath / "files" / filename
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
        return self.__log

    def get_progress_file_checksum(self):
        return self.__progress_file_checksum

    def get_write_progress(self):
        return self.__write_progress

    log = property(get_log)
    progress_file_checksum = property(get_progress_file_checksum)
    write_progress = property(get_write_progress)
    
