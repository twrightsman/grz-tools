from __future__ import annotations

import copy
import json
import logging
from pathlib import Path
from typing import Dict

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
        self._checksum = calculate_sha256(self.file_path, progress=False)

        # Possibly raises exception
        self._validate_schema()

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

    @property
    def files(self):
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
        self.__log.info('Starting checksum validation.')

        all_checksums_correct = True
        for file_path, file_info in self.files.items():
            try:
                logged_state = progress_logger.get_state(file_path)
            except FileNotFoundError:
                self.__log.error("Missing file: %s", file_path.name)
                all_checksums_correct = False
                continue

            # check if file is actually a file
            if not file_path.is_file():
                all_checksums_correct = False
                self.__log.error("Path is not a file: %s", file_path.name)
                continue

            try:
                # file state was already calculated
                expected_checksum = logged_state['expected_checksum']
                calculated_checksum = logged_state['calculated_checksum']
                checksum_correct = logged_state['checksum_correct']
            except (KeyError, TypeError) as e:
                if isinstance(e, KeyError):
                    self.__log.error("Invalid state: %s; Recalculating...", logged_state)
                elif isinstance(e, TypeError):
                    self.__log.debug("State for %s not calculated yet", file_path)

                # calculate checksum
                expected_checksum = file_info['expected_checksum']
                calculated_checksum = calculate_sha256(file_path)
                checksum_correct = calculated_checksum == expected_checksum

                progress_logger.set_state(file_path, {
                    "expected_checksum": expected_checksum,
                    "calculated_checksum": calculated_checksum,
                    "checksum_correct": checksum_correct,
                })

            if checksum_correct:
                self.__log.info(f"Checksum validated for {file_path.name}.")
            else:
                self.__log.error(
                    f"Checksum mismatch for {file_path.name}! Expected: {expected_checksum}, calculated: {calculated_checksum}"
                )
                all_checksums_correct = False

        if all_checksums_correct:
            self.__log.info('Checksum validation completed without errors.')
        else:
            self.__log.error("Checksum validation failed.")
            # TODO: print summary of failed files

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

        # The session is derived from the metadata checksum,
        # s.t. a change of the metadata file also changes the session
        self.session_dir = self.log_dir / f"metadata-{self.submission.metadata.checksum}"
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
        total_files = len(self.submission.files)
        checked_before, checked_now, failed, finished = 0, 0, 0, 0

        if stage == "validation":
            for file_info in self.submission.files.values():
                if file_info["checked"]:
                    checked_before += 1
                else:
                    checked_now += 1
                if file_info["status"] == "Finished":
                    finished += 1
                elif file_info["status"] == "Failed":
                    failed += 1

        self.__log.info(f"Summary for {stage}:")
        self.__log.info(f"Total files: {total_files}")
        self.__log.info(f"Checked before: {checked_before}")
        self.__log.info(f"Checked now: {checked_now}")
        self.__log.info(f"Failed files: {failed}")
        self.__log.info(f"Finished files: {finished}")

        if total_files == finished:
            self.__log.info(f"{stage} - Process Complete")
        else:
            self.__log.warning(f"{stage} - Process Incomplete. Address the errors before proceeding.")
