"""
Module: file_manager.py

This module contains the FileManager class which is responsible 
for managing file operations for submissions.

Classes:
- FileManager: Class to manage file operations for submissions.

Functions:
- None

Exceptions:
- FileNotFoundError: Raised if any file is not found.
- Exception: Raised for other errors during file movement or directory preparation.
"""

import json
import shutil
import logging
from pathlib import Path
from typing import List, Tuple, Dict, Any

from grz_upload.file_operations import calculate_md5

log = logging.getLogger(__name__)


class FileManager:
    """Class to manage file operations for submissions."""

    EXT = ".c4gh"

    def __init__(self) -> None:
        """Initialize FileManager."""
        self._file_invalid = 0  # Initialize file invalid count

    def copy_metadata(
        self, json_file: Dict[str, Any], metadata_file_path: Path
    ) -> None:
        """
        Move metadata to the specified file path.

        :param json_file: Dictionary containing metadata to save.
        :param metadata_file_path: Path where the metadata will be saved.
        :raises Exception: If there is an error while moving metadata.
        """
        try:
            log.info(f"Moving metadata to {metadata_file_path}")
            with open(metadata_file_path, "w", encoding="utf-8") as f:
                json.dump(json_file, f, indent=4)

        except Exception as e:
            log.error(f"Error moving metadata: {e}")
            raise

    def copy_files(self, file_paths: List[str], files_dir: Path) -> List[Path]:
        """
        Move files to the specified directory.

        :param file_paths: List of file paths to move.
        :param files_dir: Directory to move files to.
        :return: List of new file paths.
        :raises FileNotFoundError: If any file is not found.
        :raises Exception: For other errors during file movement.
        """
        try:
            new_file_paths = []
            for file_path in file_paths:
                file_name = Path(file_path).name
                new_file_path = files_dir / file_name
                log.info(f"Moving file from {file_path} to {new_file_path}")
                shutil.copy(file_path, new_file_path)
                new_file_paths.append(new_file_path)
            return new_file_paths

        except FileNotFoundError as e:
            log.error(f"File not found: {e}")
            self._file_invalid += 1
            raise
        except Exception as e:
            log.error(f"Error moving file: {e}")
            self._file_invalid += 1
            raise

    def prepare_directory(self) -> Tuple[Path, Path]:
        """
        Prepare the directory structure for submissions.

        :return: Tuple containing the paths for files directory and metadata file.
        :raises Exception: If there is an error creating directories.
        """
        try:
            path = Path.cwd() / "submission"
            path.mkdir(exist_ok=True)

            metadata_dir = path / "metadata"
            files_dir = path / "files"

            metadata_dir.mkdir(exist_ok=True)
            files_dir.mkdir(exist_ok=True)

            metadata_file = metadata_dir / "metadata.json"
            log.info(f"Directories prepared: {files_dir}, {metadata_file}")
            return files_dir, metadata_file
        except Exception as e:
            log.error(f"Error preparing directory: {e}")
            raise

    def update_file_directory(
        self, json_dict: Dict[str, Any], files_dir: Path
    ) -> List[str]:
        """
        Update the file paths in the JSON dictionary and return the updated paths.

        :param json_dict: Dictionary containing JSON data.
        :param files_dir: Directory where files are located.
        :return: List of file paths updated to the new directory.
        :raises Exception: If there is an error updating the file directory.
        """
        try:
            file_paths = []
            for donor in json_dict.get("Donors", []):
                for lab_data in donor.get("LabData", []):
                    for sequence_data in lab_data.get("SequenceData", []):
                        for files_data in sequence_data.get("files", []):
                            files_data["filepath"] = str(files_dir)
                            file_paths.append(files_dir / files_data["filename"])
            log.info(f"Updated file paths in JSON: {file_paths}")
            return file_paths
        except Exception as e:
            log.error(f"Error updating file directory: {e}")
            raise

    def validate_file(self, filename: str, filepath: str, filechecksum: str) -> None:
        """
        Validate the existence and checksum of a file.

        :param filename: Name of the file to validate.
        :param filepath: Path where the file is located.
        :param filechecksum: Expected checksum of the file.
        :raises Exception: If the file does not exist or checksum is incorrect.
        """
        fullpath = Path(filepath) / filename
        log.info(f"Validating file: {fullpath}")

        if not fullpath.is_file():
            log.error(f"The provided file {filename} in {filepath} does not exist.")
            self._file_invalid += 1
            return

        filechecksum_calc = calculate_md5(fullpath)
        if filechecksum == filechecksum_calc:
            log.info(f"Validation successful for {fullpath} - MD5 checksum is correct.")
        else:
            log.error(
                f"Validation failed for {fullpath} - MD5 checksum incorrect: "
                f"provided: {filechecksum}, calculated: {filechecksum_calc}"
            )
            self._file_invalid += 1
