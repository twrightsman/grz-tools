from __future__ import annotations
from typing import Dict, List, Tuple

import json
from pathlib import Path
import copy

from grz_upload.file_operations import read_multiple_json


class FileProgressLogger:
    """
    A class to log and track the state of files over time. It stores file states in a log file of JSON entries
    and allows querying the state based on the file path and modification time.
    """

    _index = {"file_path": str, "expected_checksum": str, "modification_time":float, "size": int} #, "modification_time": float}
    _file_states: Dict[str, Dict | List]
    # dictionary to track the progress of a file
    CHECKSUM_DICT = {"checksum_correct" : False, "calculated_checksum" : "", "status" : "fresh"}

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
                        print(index)
                        # Get state and cast them to the correct types
                        state = {k: v for k, v in row_dict.items() if k not in self._index.keys()}
    
                        self._file_states[index] = state
            else:
                raise ValueError(f"Path is not a file: '{self._file_path.name}'")

    def cleanup(self, keep: list):
        self._file_path.unlink(missing_ok=True)
        for file, expected_checksum in keep:
            state = self.get_state(file, expected_checksum)
            if state is not None: self.set_state(file, expected_checksum, self.get_state(file, expected_checksum))
        
        
    # ML: use full path as key
    def _get_index(self, file_path: Path, expected_checksum: str) -> Tuple:
        """
        Generates a unique index for a given file based on its name and modification time.

        :param file_path: Path object representing the file.
        :return: A tuple containing the file name and modification time.
        """
        if file_path.is_file():
            return str(file_path), expected_checksum, file_path.stat().st_mtime, file_path.stat().st_size
        else:
            return str(file_path), expected_checksum, 0, 0 # catches files that do not exist
    
    def get_index(self, file_path: Path, expected_checksum: str) -> Tuple:
        return self._get_index(file_path, expected_checksum)

    def get_state(self, file_path: str | Path, expected_checksum: str) -> Dict | None:
        """
        Retrieves the stored state of a file if it exists in the log.

        :param file_path: The file path to query for the state.
        :return: A dictionary representing the file's state, or None if the file's state isn't logged.
        """
        file_path = Path(file_path)
        index = self._get_index(file_path, expected_checksum)
        return copy.deepcopy(self._file_states.get(index, None))

    def set_state(self, file_path: str | Path, expected_checksum: str, state: Dict):
        """
        Log the state of a file:
         - Update the in-memory state
         - Persist the state to the JSON log file

        :param file_path: The path of the file whose state is being set.
        :param state: A dictionary containing the file's state data to be logged.
        """
        file_path = Path(file_path)
        index = self._get_index(file_path, expected_checksum)
        # Update state in memory
        self._file_states[index] = state
        # Persist state to JSON log file
        with open(self._file_path, "a", newline='') as fd:
            # Append the new state row to the log file
            json.dump({
                # index keys
                **{k: v for k, v in zip(self._index.keys(), index)},
                # state
                **state,
            }, fd)
            fd.write("\n")

    def get_count_file_states(self) -> int:
        """
        Returns the number of entries in the file_states dictionary
        :return: An integer representing the number of entries in the file_states dictionary
        """
        return len(self._file_states)

    def get_files_not_written(self) -> list:
        """
        Returns the files which have not been written back into the progress file
        : return: A list with the file paths of files which are not written back into the progress file
        """
        return [i for i in self._file_states if not self._file_states[i]["write_back"]]

