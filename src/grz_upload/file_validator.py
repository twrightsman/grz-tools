import logging
from pathlib import Path

from grz_upload.file_operations import calculate_sha256

log = logging.getLogger(__name__)


class FileValidator:
    def __init__(self, folderpath):
        self.folderpath = Path(folderpath)

    def validate_file(self, filename, expected_checksum):
        """Validate the existence and checksum of a file."""
        fullpath = self.folderpath / "files" / filename
        if not fullpath.is_file():
            log.error(f"The provided file {filename} does not exist.")
            return False, None

        calculated_checksum = calculate_sha256(fullpath)
        if calculated_checksum != expected_checksum:
            log.error(
                f"Checksum mismatch for {filename}: "
                f"expected {expected_checksum}, got {calculated_checksum}"
            )
            return False, calculated_checksum

        log.info(f"Checksum for {filename} is valid.")
        return True