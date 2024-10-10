from __future__ import annotations

import copy
import json
import logging
from pathlib import Path
from typing import Dict
from collections import defaultdict

import jsonschema

from grz_upload.constants import GRZ_METADATA_JSONSCHEMA
from grz_upload.file_operations import Crypt4GH, calculate_sha256, is_relative_subdirectory
from grz_upload.progress_logging import FileProgressLogger



log = logging.getLogger(__name__)


class SubmissionMetadata:
    __log = log.getChild("SubmissionMetadata")

    def __init__(self, metadata_file):
        """
        Class for reading and validating submission metadata

        :param metadata_file: path to the metadata.json file
        :raises json.JSONDecodeError: if failed to read the metadata.json file
        :raises jsonschema.exceptions.ValidationError: if metadata does not match expected schema
        """
        self.file_path = metadata_file
        self.content = self._read_metadata(self.file_path)
        self._checksum = calculate_sha256(self.file_path)

        # Possibly raises exception
        # self._validate_schema()

        self._files = None

    @classmethod
    def _read_metadata(cls, file_path: Path) -> dict:
        """
        Load and parse the metadata file in JSON format.

        :param file_path: Path to the metadata JSON file
        :return: Parsed metadata as a dictionary
        """
        try:
            with open(file_path, "r", encoding="utf-8") as jsonfile:
                metadata = json.load(jsonfile)
                return metadata
        except json.JSONDecodeError as e:
            cls.__log.error("Invalid JSON format in metadata file: %s", file_path)
            raise e

    def _validate_schema(self, schema=GRZ_METADATA_JSONSCHEMA):
        """
        Validate the schema of the content

        :param schema:
        :return:
        """
        try:
            jsonschema.validate(self.content, schema=schema)
        except jsonschema.exceptions.ValidationError as e:
            self.__log.error("Invalid JSON schema in metadata file '%s'", self.file_path)
            raise e

    @property
    def files(self) -> Dict:
        """
        The files liked in the metadata.

        :return: Dictionary of `file_path` -> `file_info` pairs.
            Each `file_path` refers to the relative file path from the metadata.
        """
        if self._files is not None:
            return self._files

        submission_files = {}
        for donor in self.content.get("donors", []):
            for lab_data in donor.get("labData", []):
                for sequence_data in lab_data.get("sequenceData", []):
                    for file_data in sequence_data.get("files", []):
                        relative_file_path = file_data["filePath"]

                        file_info = {
                            'valid': False,
                            'expected_checksum': file_data["fileChecksum"]
                        }

                        # check if file path is actually relative to the submission files directory
                        if not is_relative_subdirectory(relative_file_path, "./"):
                            file_info["invalid_reason"] = f"Path is not relative"
                            submission_files[relative_file_path] = file_info
                            continue

                        # check if file is already linked
                        if relative_file_path in submission_files:
                            file_info["invalid_reason"] = "Duplicate filename in metadata"
                            submission_files[relative_file_path] = file_info
                            continue

                        file_info["valid"] = True
                        submission_files[relative_file_path] = file_info

        self._files = submission_files
        return submission_files

    def validate(self):
        self.__log.debug(self.files)
        all_valid = True
        for file_path, file_info in self.files.items():
            if not file_info["valid"]:
                self.__log.error("%s: %s", file_path, file_info["invalid_reason"])

                all_valid = False

        if not all_valid:
            raise SubmissionValidationError("Submission validation failed. Check log file for details.")

    @property
    def checksum(self) -> str:
        """
        Checksum of the metadata file
        """
        return self._checksum


class SubmissionValidationError(Exception):
    pass


class Submission:
    __log = log.getChild("Submission")

    def __init__(self, metadata_dir: str | Path, files_dir: str | Path):
        self.metadata_dir = Path(metadata_dir)
        self.files_dir = Path(files_dir)

        self.metadata = SubmissionMetadata(self.metadata_dir / "metadata.json")
        self.__progress_stat = defaultdict(list)

    def get_stats(self) -> Dict:
        return self.__progress_stat

    @property
    def files(self):
        """
        Dictionary with file path as key, file information as value
        """
        retval = {}
        for file_path, file_info in self.metadata.files.items():
            absolute_file_path = self.files_dir / file_path
            file_info["relative_path"] = file_path

            retval[absolute_file_path] = file_info
        return retval

    def validate_checksums(self, progress_log_file: str | Path):
        """
        Validate the checksum of files against those in the metadata.
        """
        progress_logger = FileProgressLogger(
            log_file_path=progress_log_file
        )
        # fields:
        # - "expected_checksum": str
        # - "calculated_checksum": str
        # - "checksum_correct": bool
        fail_list = []
        self.__log.info('Starting checksum validation.')
        all_checksums_correct = True
        progress_logger.cleanup(keep = [(i, self.files[i]["expected_checksum"]) for i in self.files]) # loescht Datei, ueberschreibt sie in set_state
        for file_path, file_info in self.files.items():
            self.__progress_stat['total'].append(file_path)
            # check if file is actually a file, and skip if not
            if not file_path.is_file():
                all_checksums_correct = False
                self.__log.error("Path is not a file: %s", file_path.name)
                self.__progress_stat['file_not_found'].append(file_path)
                continue
            
            # return is either None or a dictionary. None means, the file has not been tracked before
            logged_state = progress_logger.get_state(file_path, file_info["expected_checksum"])
            expected_checksum = file_info["expected_checksum"]
            if logged_state is None:
                self.__log.warning("File %s has not been processing before.", file_path)
                logged_state = copy.deepcopy(FileProgressLogger.CHECKSUM_DICT) # get_state returns by default either a dictionary or None; the previously written try except FileNotFoundError would have never been raised

            if logged_state["status"] == "fresh":
                # new file, so process everything
                # self.__progress_stat['new'] += 1
                # if previous_processed != 0:
                #     self.__log.warning("Folder has been processed before, File %s has been added after initial processing and checksum not calculated yet", file_path)
                # else:
                #     self.__progress_stat['old'] += 1
                #     self.__log.debug("Folder has not been processed before, File %s is fresh and checksum not calculated yet", file_path)

                calculated_checksum = calculate_sha256(file_path)
                checksum_correct = expected_checksum == calculated_checksum
                logged_state["checksum_correct"] = checksum_correct
                if checksum_correct:
                    logged_state["calculated_checksum"] = calculated_checksum
                    logged_state["status"] = "processed"
                    self.__progress_stat['processed'].append(file_path)
                else:
                    logged_state["calculated_checksum"] = ""
                    logged_state["status"] = "failed"
                    self.__progress_stat['failed'].append(file_path)
            elif logged_state["status"] in ("processed", "failed"): # checks for previously processed files
                redo = False
                if logged_state["calculated_checksum"] != file_info['expected_checksum']:
                    self.__log.warning("File %s has been processed before, previously recorded checksum does not match, recalculating ... ", file_path)
                    redo = True
                else:
                    self.__log.debug("File %s has been processed before, looks okay ... ", file_path)
                    checksum_correct = True

                if redo:
                    calculated_checksum = calculate_sha256(file_path)
                    expected_checksum = file_info["expected_checksum"]
                    checksum_correct = expected_checksum == calculated_checksum
                    logged_state["checksum_correct"] = checksum_correct

                    if checksum_correct:
                        logged_state["calculated_checksum"] = calculated_checksum
                        logged_state["status"] = "processed"
                        self.__progress_stat['processed'].append(file_path)
                    else:
                        logged_state["status"] = "failed"
                        self.__progress_stat['failed'].append(file_path)
                else:
                    logged_state["status"] = "processed"
                    self.__progress_stat['processed'].append(file_path)

            if checksum_correct:
                progress_logger.set_state(file_path, expected_checksum, logged_state)
                self.__log.info(f"Checksum validated for {file_path.name}.")
            else:
                self.__log.error(f"Checksum validation for {file_path} failed. Expected checksum: {expected_checksum}, calculated: {calculated_checksum}")
                progress_logger.set_state(file_path, expected_checksum, logged_state)
                fail_list.append(file_path)
                all_checksums_correct = False

        # files_not_written = progress_logger.get_files_not_written()
        # if len(files_not_written) != 0:
        #     self.__progress_stat['ignored'] += len(files_not_written)
        #     self.__log.info("The following files were not listed in the current metadata file but in the previous one. They are being removed from a next possible check:")
        #     for i in files_not_written: self.__log.info(i)

        if all_checksums_correct:
            self.__log.info('Checksum validation completed without errors.')
        else:
            self.__log.error("Checksum validation failed. See erros above for the following files:")
            for i in self.__progress_stat["file_not_found"]: self.__log.error("File not found: %s", i)
            for i in self.__progress_stat["failed"]: self.__log.error("Checksum invalid: %s", i)
            raise SubmissionValidationError("Submission validation failed. Check log file for details.")

    def encrypt(
            self,
            encrypted_files_dir: str | Path,
            public_key_file_path: str | Path,
            progress_log_file: str | Path
    ) -> EncryptedSubmission:
        """
        Encrypt this submission with a public key using Crypt4Gh

        :param encrypted_files_dir: Output directory of the encrypted files
        :param public_key_file_path: Path to the public key file
        :param progress_log_file: Path to a log file to store the progress of the encryption process
        :return: EncryptedSubmission instance
        """
        encrypted_files_dir = Path(encrypted_files_dir)

        progress_logger = FileProgressLogger(
            log_file_path=progress_log_file
        )

        try:
            public_keys = Crypt4GH.prepare_c4gh_keys(public_key_file_path)
        except Exception as e:
            self.__log.error(f"Error preparing public keys: {e}")
            raise e

        for file_path, file_info in self.files.items():
            # encryption_successful = True
            try:
                logged_state = progress_logger.get_state(file_path)
            except FileNotFoundError as e:
                self.__log.error("Missing file: %s", file_path.name)

                # TODO: Do we want to raise an exception and stop here or
                #  do we want to continue with the remaining files?
                # encryption_successful = False
                # continue
                raise e

            self.__log.info("Encrypting file: %s", file_path.name)
            encrypted_file_path = EncryptedSubmission.get_encrypted_file_path(file_path)
            # TODO: write header to separate file
            # encryption_header_path = EncryptedSubmission.get_encrypted_file_path(file_path)

            try:
                Crypt4GH.encrypt_file(file_path, encrypted_file_path, public_keys)

                self.__log.info(
                    f"Encryption complete for {file_path.name}. "
                )
            except Exception as e:
                self.__log.error("Encryption failed for '%s'", file_path.name)

                raise e

        self.__log.info("File encryption completed.")

        return EncryptedSubmission(metadata_dir=self.metadata_dir, encrypted_files_dir=encrypted_files_dir)


class EncryptedSubmission:
    __log = log.getChild("EncryptedSubmission")

    def __init__(self, metadata_dir: str | Path, encrypted_files_dir: str | Path):
        self.metadata_dir = Path(metadata_dir)
        self.encrypted_files_dir = Path(encrypted_files_dir)

        self.metadata = SubmissionMetadata(self.metadata_dir / "metadata.json")

    @property
    def encrypted_files(self):
        retval = {}
        for file_path, file_info in self.metadata.files.items():
            encrypted_file_path = self.get_encrypted_file_path(self.encrypted_files_dir / file_path)

            encrypted_file_info = copy.deepcopy(file_info)
            encrypted_file_info["original_path"] = file_path
            encrypted_file_info["type"] = "file"

            retval[encrypted_file_path] = encrypted_file_info

            # TODO: add header path as separate file
            # encryption_header_path = self.get_encryption_header_path(self.encrypted_files_dir / file_path)
            #
            # encryption_header_info = copy.deepcopy(file_info)
            # encryption_header_info["original_path"] = file_path
            # encryption_header_info["type"] = "header"
            #
            # retval[encryption_header_path] = encryption_header_info

        return retval

    @staticmethod
    def get_encrypted_file_path(file_path: str | Path) -> Path:
        return Path(file_path).with_suffix(".c4gh")

    @staticmethod
    def get_encryption_header_path(file_path: str | Path) -> Path:
        return Path(file_path).with_suffix(".c4gh_header")

    def decrypt(self) -> Submission:
        raise NotImplementedError()


class Worker:
    __log = log.getChild("Worker")

    def __init__(self, submission_dir: str | Path, working_dir: str | Path = None):
        submission_dir = Path(submission_dir)
        if working_dir is not None:
            working_dir = Path(working_dir)
        else:
            working_dir = submission_dir

        self.submission = Submission(
            metadata_dir=submission_dir / "metadata",
            files_dir=submission_dir / "files",
        )
        self.encrypted_files_dir = working_dir / "encrypted_files"
        self.log_dir = working_dir / "logs"
        if not self.log_dir.is_dir(): self.log_dir.mkdir(mode = 0o770, parents = True, exist_ok = True)

        # The session is derived from the metadata checksum,
        # s.t. a change of the metadata file also changes the session
        self.session_dir = self.log_dir # / f"metadata-{self.submission.metadata.checksum}"
        self.session_dir.mkdir(mode = 0o770, parents = True, exist_ok = True)
        self.__log.info("Session directory: %s", self.session_dir)

        self.progress_file_checksum = self.session_dir / "progress_checksum.json"
        self.progress_file_encrypt = self.session_dir / "progress_encrypt.json"
        self.progress_file_upload = self.session_dir / "progress_upload.json"

    def validate(self):
        """
        Validate this submission

        :raises SubmissionValidationError: if the validation fails
        """
        self.submission.metadata.validate()
        self.submission.validate_checksums(progress_log_file=self.progress_file_checksum)
        # TODO: validate FASTQ

    def encrypt(self, public_key_file_path: str | Path) -> EncryptedSubmission:
        """
        Encrypt this submission with a public key using Crypt4Gh.

        :return: EncryptedSubmission instance
        """
        encrypted_submission = self.submission.encrypt(
            encrypted_files_dir=self.encrypted_files_dir,
            public_key_file_path=public_key_file_path,
            progress_log_file=self.progress_file_encrypt,
        )
        return encrypted_submission

    def show_summary(self, stage: str):
        """
        Display the summary of file processing for the specified stage.
        :param stage: The current processing stage (e.g., 'checksum', 'encryption').
        """
        # TODO: update this method
        file_stats = self.submission.get_stats()
        
        checked_before, checked_now, failed, finished = 0, 0, 0, 0

        if stage == "validate":
            info_text = "SHA256 checksum validation"

        self.__log.info(info_text + " - overview:")
        self.__log.info(f"Total files: {file_stats['total']}")
        self.__log.info(f"Added in previous run: {file_stats['old']}")
        self.__log.info(f"Added in current run: {file_stats['new']}")
        self.__log.error(f"Files not found: {file_stats['file_not_found']}") if file_stats['file_not_found'] != 0 else self.__log.info(f"Files not found: {file_stats['file_not_found']}")
        self.__log.error(f"Failed files: {file_stats['failed']}") if file_stats['failed'] != 0 else self.__log.info(f"Failed files: {file_stats['failed']}")  
        self.__log.info(f"Finished files: {file_stats['processed']}")

        if {file_stats['total']} == {file_stats['processed']}:
            self.__log.info(f"{info_text} - Process Complete")
        else:
            self.__log.warning(f"{info_text} - Process Incomplete. Address the errors before proceeding.")
