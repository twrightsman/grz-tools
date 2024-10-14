from __future__ import annotations
from typing import Dict, List, Tuple

import json
from pathlib import Path
import copy

from grz_upload.file_operations import read_multiple_json
import grz_upload.parser as parser


class FileProgressLogger:
    """
    A class to log and track the state of files over time. It stores file states in a log file of JSON entries
    and allows querying the state based on the file path and modification time.
    """

    _index = {
        "file_path": str,
        "modification_time": float,
        "size": int
    }
    # mapping of index -> (metadata, data)
    _file_states: Dict[Tuple[str, float, int], Tuple[Dict, Dict | List]]

    def __init__(self, log_file_path: str | Path):
        """
        Initializes the FileProgressLogger instance.

        :param fields: Dictionary of field names and their types, representing the state to track.
        :param log_file_path: Path to the JSON file where file progress will be logged.
        """
        self._file_path = Path(log_file_path)
        self._file_states = {}

        # Read existing file states from the log file
        self.read()

    def read(self):
        """
        Reads the log file and loads the file states into memory.

        :raises ValueError: If the path exists but is not a file.
        """
        if self._file_path.exists():
            if self._file_path.is_file():
                with open(self._file_path, "r") as fd:
                    for row_dict in read_multiple_json(fd):
                        # Get index and cast them to the correct types
                        index = tuple(self._index[k](row_dict[k]) for k in self._index.keys())
                        # Get metadata
                        metadata = row_dict["metadata"]
                        # Get state
                        state = row_dict["state"]

                        self._file_states[index] = (metadata, state)
            else:
                raise ValueError(f"Path is not a file: '{str(self._file_path)}'")

    def cleanup(self, keep: List[Tuple[str | Path, Dict | parser.SubmissionFileMetadata]]):
        self._file_path.unlink(missing_ok=True)
        for file, file_metadata in keep:
            state = self.get_state(file, file_metadata)
            if state is not None:
                self.set_state(file, file_metadata, self.get_state(file, file_metadata))

    def _get_index(self, file_path: str | Path) -> Tuple[str, float, int]:
        """
        Generates a unique index for a given file based on its name and modification time.

        :param file_path: Path object representing the file.
        :return: A tuple containing the file name and modification time.
        """
        file_path = Path(file_path).resolve()

        if file_path.is_file():
            return str(file_path), file_path.stat().st_mtime, file_path.stat().st_size
        else:

            return str(file_path), -1, -1  # catches files that do not exist

    # def get_index(self, file_path: Path, file_metadata: Dict) -> Tuple:
    #     return self._get_index(file_path, file_metadata)

    def get_state(self, file_path: str | Path, file_metadata: Dict | parser.SubmissionFileMetadata) -> Dict | None:
        """
        Retrieves the stored state of a file if it exists in the log.

        :param file_path: The file path to query for the state.
        :return: A dictionary representing the file's state, or None if the file's state isn't logged.
        """
        file_path = Path(file_path)
        index = self._get_index(file_path)

        # convert to metadata object if necessary
        if not isinstance(file_metadata, parser.SubmissionFileMetadata):
            file_metadata = parser.SubmissionFileMetadata.from_json_dict(file_metadata)

        # get stored state
        stored_metadata, stored_data = self._file_states.get(index, (None, None))

        # check if metadata matches
        if stored_metadata is not None:
            # convert stored_metadata to SubmissionFileMetadata
            stored_metadata = parser.SubmissionFileMetadata.from_json_dict(stored_metadata)
            if file_metadata == stored_metadata:
                return copy.deepcopy(stored_data)
        # metadata mismatch -> no valid stored state -> return None
        return None

    def set_state(self, file_path: str | Path, file_metadata: Dict | parser.SubmissionFileMetadata, state: Dict):
        """
        Log the state of a file:
         - Update the in-memory state
         - Persist the state to the JSON log file

        :param file_path: The path of the file whose state is being set.
        :param file_metadata: Submission file metadata to store
        :param state: A dictionary containing the file's state data to be logged.
        """
        file_path = Path(file_path)
        index = self._get_index(file_path)

        # convert to metadata object if necessary
        if not isinstance(file_metadata, parser.SubmissionFileMetadata):
            file_metadata = parser.SubmissionFileMetadata.from_json_dict(file_metadata)

        # Update state in memory
        self._file_states[index] = (file_metadata.to_json_dict(), state)

        # Persist state to JSON log file
        with open(self._file_path, "a", newline='') as fd:
            # Append the new state row to the log file
            json.dump({
                # index keys
                **{k: v for k, v in zip(self._index.keys(), index)},
                # state
                "metadata": file_metadata.to_json_dict(),
                "state": state,
            }, fd)
            fd.write("\n")

    def num_entries(self) -> int:
        """
        Returns the number of entries in the file_states dictionary

        :return: An integer representing the number of entries in the file_states dictionary
        """
        return len(self._file_states)
