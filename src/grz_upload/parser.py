from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, override

import jsonschema

from grz_upload.constants import GRZ_METADATA_JSONSCHEMA
from grz_upload.file_operations import Crypt4GH, calculate_sha256
from grz_upload.progress_logging import FileProgressLogger

log = logging.getLogger(__name__)


@dataclass
class SubmissionFileMetadata:
    """
    Dataclass for submission file metadata. Contains the following properties:
        - filePath: Path relative to the submission root, e.g.: sequencing_data/patient_001/patient_001_dna.bam
        - fileType: Type of the file; one of: ["bam", "vcf", "bed", "fastq"]
        - fileChecksum: Checksum of the file (expected value)
        - fileSizeInBytes: Size of the file in bytes
        - checksumType: Type of checksum algorithm used, defaults to "sha256"
    """

    filePath: Path
    fileType: str
    fileChecksum: str
    fileSizeInBytes: int
    checksumType: str = "sha256"

    _SUPPORTED_CHECKSUM_TYPES = {"sha256"}
    _VALID_FILE_TYPES = {"bam", "vcf", "bed", "fastq"}
    _VALID_FASTQ_FILE_EXTENSIONS = {".fastq", ".fastq.gz"}
    _VALID_BED_FILE_EXTENSIONS = {".bed", ".bed.gz"}
    _VALID_VCF_FILE_EXTENSIONS = {".vcf", ".vcf.gz", ".vcf.bgz", ".bcv"}

    def validate_metadata(self) -> Generator[str]:
        """
        Validates whether the metadata is correct and yields errors in the metadata.
        :return: Generator of strings describing the errors
        """
        # check if checksum type is correct
        if self.checksumType not in self._SUPPORTED_CHECKSUM_TYPES:
            yield (f"{str(self.filePath)}: Unsupported checksum type: {self.checksumType}. "
                   f"Supported types: {self._SUPPORTED_CHECKSUM_TYPES}")

        # check if file type is correct
        if self.fileType not in self._VALID_FILE_TYPES:
            yield f"Unsupported file type: {self.fileType}"

        # check if file extension is correct
        if self.fileType == "fastq":
            if not any(str(self.filePath).endswith(suffix) for suffix in self._VALID_FASTQ_FILE_EXTENSIONS):
                yield (f"{str(self.filePath)}: Unsupported FASTQ file extensions! "
                       f"Valid extensions: {', '.join(self._VALID_FASTQ_FILE_EXTENSIONS)}")
        elif self.fileType == "bam" and not str(self.filePath).endswith(".bam"):
            yield f"{str(self.filePath)}: Unsupported BAM file extensions! Must end with '.bam'"
        elif self.fileType == "bed" and not str(self.filePath).endswith(".bed"):
            if not any(str(self.filePath).endswith(suffix) for suffix in self._VALID_BED_FILE_EXTENSIONS):
                yield (f"{str(self.filePath)}: Unsupported BED file extensions! "
                       f"Valid extensions: {', '.join(self._VALID_BED_FILE_EXTENSIONS)}")
        elif self.fileType == "vcf":
            if not any(str(self.filePath).endswith(suffix) for suffix in self._VALID_VCF_FILE_EXTENSIONS):
                yield (f"{str(self.filePath)}: Unsupported VCF file extensions! "
                       f"Valid extensions: {', '.join(self._VALID_VCF_FILE_EXTENSIONS)}")

    def validate_data(self, local_file_path: Path) -> Generator[str]:
        """
        Validates whether the provided file matches this metadata.

        :param local_file_path: Path to the actual file (resolved if symlinked)
        :return: Generator of errors
        """
        # Resolve file path
        local_file_path = local_file_path.resolve()

        # Check if path exists
        if not local_file_path.exists():
            yield f"{str(self.filePath)} does not exist!"
            # Return here as following tests cannot work
            return

        # Check if path is a file
        if not local_file_path.is_file():
            yield f"{str(self.filePath)} is not a file!"
            # Return here as following tests cannot work
            return

        # Check if the checksum is correct
        if self.checksumType == "sha256":
            calculated_checksum = calculate_sha256(local_file_path)
            if self.fileChecksum != calculated_checksum:
                yield (f"{str(self.filePath)}: Checksum mismatch! "
                       f"Expected: '{self.fileChecksum}', calculated: '{calculated_checksum}'.")
        else:
            yield (f"{str(self.filePath)}: Unsupported checksum type: {self.checksumType}. "
                   f"Supported types: {self._SUPPORTED_CHECKSUM_TYPES}")

        # Check file size
        if self.fileSizeInBytes != local_file_path.stat().st_size:
            yield (f"{str(self.filePath)}: File size mismatch! "
                   f"Expected: '{self.fileSizeInBytes}', observed: '{local_file_path.stat().st_size}'.")

    @classmethod
    def from_json_dict(cls, data: dict):
        """
        Create a SubmissionFileMetadata object from a JSON-like dictionary.

        :param data: JSON-like dictionary
        """
        required_keys = {"filePath", "fileType", "fileChecksum", "fileSizeInBytes"}
        missing_keys = required_keys - data.keys()
        if missing_keys:
            raise ValueError(f"Missing required keys: {', '.join(missing_keys)}")

        return cls(
            filePath=Path(data["filePath"]),
            fileType=data["fileType"],
            fileChecksum=data["fileChecksum"],
            fileSizeInBytes=data["fileSizeInBytes"],
            checksumType=data.get("checksumType", "sha256"),
        )

    def to_json_dict(self):
        return {
            "filePath": str(self.filePath),
            "fileType": self.fileType,
            "fileChecksum": self.fileChecksum,
            "fileSizeInBytes": self.fileSizeInBytes,
            "checksumType": self.checksumType,
        }


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
        :raises json.JSONDecodeError: if failed to read the metadata.json file
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

        :param schema: path to JSON schema file
        :raises jsonschema.exceptions.ValidationError: if schema does not match expected schema
        """
        try:
            jsonschema.validate(self.content, schema=schema)
        except jsonschema.exceptions.ValidationError as e:
            self.__log.error("Invalid JSON schema in metadata file '%s'", self.file_path)
            raise e

    @property
    def files(self) -> Dict[Path, SubmissionFileMetadata]:
        """
        The files liked in the metadata.

        :return: Dictionary of `file_path` -> `SubmissionFileMetadata` pairs.
            Each `file_path` refers to the relative file path from the metadata.
        """
        if self._files is not None:
            return self._files

        submission_files = {}
        for donor in self.content.get("Donors", []):
            for lab_data in donor.get("labData", []):
                for sequence_data in lab_data.get("sequenceData", []):
                    for file_data in sequence_data.get("files", []):
                        file_metadata = SubmissionFileMetadata.from_json_dict(file_data)

                        submission_files[file_metadata.filePath] = file_metadata

        self._files = submission_files
        return self._files

    def validate(self) -> Generator[str]:
        """
        Validates this submission metadata.

        :return: Generator of errors
        """
        submission_files = {}
        for donor in self.content.get("Donors", []):
            for lab_data in donor.get("labData", []):
                for sequence_data in lab_data.get("sequenceData", []):
                    for file_data in sequence_data.get("files", []):
                        file_metadata = SubmissionFileMetadata.from_json_dict(file_data)

                        # check if file is already registered
                        if other_metadata := submission_files.get(file_metadata.filePath):
                            # check if metadata matches
                            if file_metadata != other_metadata:
                                yield f"{file_metadata.filePath}: Different metadata for the same path observed!"

                            # check if FASTQ data was already linked in another submission
                            if file_metadata.fileType == "fastq":
                                yield f"{file_metadata.filePath}: FASTQ file already linked in another submission!"
                            if file_metadata.fileType == "bam":
                                yield f"{file_metadata.filePath}: BAM file already linked in another submission!"
                        else:
                            submission_files[file_metadata.filePath] = file_metadata
                            # check if the file metadata itself is correct
                            yield from file_metadata.validate_metadata()

    @property
    def checksum(self) -> str:
        """
        Checksum of the metadata file
        """
        return self._checksum


class Submission:
    __log = log.getChild("Submission")

    def __init__(self, metadata_dir: str | Path, files_dir: str | Path):
        self.metadata_dir = Path(metadata_dir)
        self.files_dir = Path(files_dir)

        self.metadata = SubmissionMetadata(self.metadata_dir / "metadata.json")

    @property
    def files(self) -> Dict[Path, SubmissionFileMetadata]:
        """
        The files liked in the metadata.

        :return: Dictionary of `local_file_path` -> `SubmissionFileMetadata` pairs.
        """
        retval = {}
        for file_path, file_metadata in self.metadata.files.items():
            local_file_path = self.files_dir / file_path

            retval[local_file_path] = file_metadata

        return retval

    def validate_checksums(self, progress_log_file: str | Path) -> Generator[str]:
        """
        Validates the checksum of the files against the metadata and prints the errors.

        :return: Generator of errors
        """
        progress_logger = FileProgressLogger(
            log_file_path=progress_log_file
        )
        # cleanup log file and keep only files listed here
        progress_logger.cleanup(
            keep=[(file_path, file_metadata) for file_path, file_metadata in self.files.items()]
        )
        # fields:
        # - "errors": List[str]
        # - "validation_passed": bool

        for local_file_path, file_metadata in self.files.items():
            logged_state = progress_logger.get_state(local_file_path, file_metadata)

            # determine if we can skip the verification
            if logged_state is None:
                self.__log.debug("State for %s not calculated yet", local_file_path)
            elif not logged_state.get("validation_passed", False):
                errors = logged_state.get("errors", [])
                yield from errors

                # skip re-verification
                continue
            else:
                self.__log.debug("Validation for %s already passed, skipping...", str(local_file_path))

                # skip re-verification
                continue

            self.__log.debug("Validating '%s'...", str(local_file_path))
            # validate the file
            errors = list(file_metadata.validate_data(local_file_path))
            validation_passed = len(errors) == 0

            # log state
            progress_logger.set_state(
                local_file_path,
                file_metadata,
                state={
                    "errors": errors,
                    "validation_passed": validation_passed,
                }
            )

            yield from errors

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

        for file_path, file_metadata in self.files.items():
            # encryption_successful = True
            logged_state = progress_logger.get_state(file_path, file_metadata)

            if logged_state is None or not logged_state.get("encryption_successful", False):
                self.__log.info("Encrypting file: %s", str(file_path))
                encrypted_file_path = EncryptedSubmission.get_encrypted_file_path(file_path)

                try:
                    Crypt4GH.encrypt_file(file_path, encrypted_file_path, public_keys)

                    self.__log.info(
                        f"Encryption complete for {str(file_path)}. "
                    )
                    progress_logger.set_state(
                        file_path,
                        file_metadata,
                        state={
                            "encryption_successful": True
                        }
                    )
                except Exception as e:
                    self.__log.error("Encryption failed for '%s'", str(file_path))

                    progress_logger.set_state(
                        file_path,
                        file_metadata,
                        state={
                            "encryption_successful": False,
                            "error": str(e)
                        }
                    )

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
        for file_path, file_metadata in self.metadata.files.items():
            encrypted_file_path = self.get_encrypted_file_path(self.encrypted_files_dir / file_path)

            retval[encrypted_file_path] = file_metadata

        return retval

    @staticmethod
    def get_encrypted_file_path(file_path: str | Path) -> Path:
        p = Path(file_path)
        return p.with_suffix(p.suffix + ".c4gh")

    @staticmethod
    def get_encryption_header_path(file_path: str | Path) -> Path:
        return Path(file_path).with_suffix(".c4gh_header")

    def decrypt(self) -> Submission:
        raise NotImplementedError()


class SubmissionValidationError(Exception):
    pass


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
        if not self.log_dir.is_dir():
            self.log_dir.mkdir(mode=0o770, parents=True, exist_ok=True)

        self.log_dir.mkdir(mode=0o770, parents=True, exist_ok=True)
        self.__log.info("Log directory: %s", self.log_dir)

        self.progress_file_checksum = self.log_dir / "progress_checksum.cjson"
        self.progress_file_encrypt = self.log_dir / "progress_encrypt.cjson"
        self.progress_file_upload = self.log_dir / "progress_upload.cjson"

    def validate(self):
        """
        Validate this submission

        :raises SubmissionValidationError: if the validation fails
        """

        self.__log.info('Starting metadata validation...')
        if errors := list(self.submission.metadata.validate()):
            error_msg = "\n".join([
                'Metadata validation failed! Errors:',
                *errors
            ])
            self.__log.error(error_msg)

            raise SubmissionValidationError(error_msg)
        else:
            self.__log.info('Metadata validation successful!')

        self.__log.info('Starting checksum validation...')
        if errors := list(self.submission.validate_checksums(progress_log_file=self.progress_file_checksum)):
            error_msg = "\n".join([
                'Checksum validation failed! Errors:',
                *errors
            ])
            self.__log.error(error_msg)

            raise SubmissionValidationError(error_msg)
        else:
            self.__log.info('Checksum validation successful!')

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

    # def show_summary(self, stage: str):
    #     """
    #     Display the summary of file processing for the specified stage.
    #     :param stage: The current processing stage (e.g., 'checksum', 'encryption').
    #     """
    #     # TODO: update this method
    #     file_stats = self.submission.get_stats()
    #
    #     checked_before, checked_now, failed, finished = 0, 0, 0, 0
    #
    #     if stage == "validate":
    #         info_text = "SHA256 checksum validation"
    #
    #     self.__log.info(info_text + " - overview:")
    #     self.__log.info(f"Total files: {file_stats['total']}")
    #     self.__log.info(f"Added in previous run: {file_stats['old']}")
    #     self.__log.info(f"Added in current run: {file_stats['new']}")
    #     self.__log.error(f"Files not found: {file_stats['file_not_found']}") if file_stats['file_not_found'] != 0 else self.__log.info(f"Files not found: {file_stats['file_not_found']}")
    #     self.__log.error(f"Failed files: {file_stats['failed']}") if file_stats['failed'] != 0 else self.__log.info(f"Failed files: {file_stats['failed']}")
    #     self.__log.info(f"Finished files: {file_stats['processed']}")
    #
    #     if {file_stats['total']} == {file_stats['processed']}:
    #         self.__log.info(f"{info_text} - Process Complete")
    #     else:
    #         self.__log.warning(f"{info_text} - Process Incomplete. Address the errors before proceeding.")
