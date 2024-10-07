import pytest
import tempfile
import os
from pathlib import Path
import json
from unittest.mock import patch

from grz_upload.file_operations import read_multiple_json
from grz_upload.progress_logging import FileProgressLogger


@pytest.fixture
def temp_log_file():
    """Fixture to create a temporary log file."""
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.close()
    yield Path(temp_file.name)
    os.remove(temp_file.name)


@pytest.fixture
def temp_file():
    """Fixture to create a temporary file to track."""
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.close()
    yield Path(temp_file.name)
    os.remove(temp_file.name)


@pytest.fixture
def logger(temp_log_file):
    """Fixture to create a FileProgressLogger instance with sample fields."""
    return FileProgressLogger(temp_log_file)


def test_set_and_get_state(logger, temp_file):
    """Test that setting and getting the state works."""
    # Set state for a file
    state = {"progress": 50, "status": "in_progress"}
    logger.set_state(temp_file, state)

    # Retrieve the state
    retrieved_state = logger.get_state(temp_file)
    assert retrieved_state == state


def test_get_state_file_not_tracked(logger, temp_file):
    """Test that get_state returns None for untracked files."""
    # Retrieve the state
    retrieved_state = logger.get_state(temp_file)
    assert retrieved_state is None


def test_persist_state_to_json(logger, temp_log_file, temp_file):
    """Test that file state is persisted to json."""
    state = {"progress": 75, "status": "completed"}
    logger.set_state(temp_file, state)

    # Check that the state was appended to the json file
    with open(temp_log_file, "r") as f:
        entries = list(read_multiple_json(f))
        assert len(entries) == 1
        assert entries[0] == {
            "file_path": temp_file.name,
            "modification_time": temp_file.stat().st_mtime,
            "progress": 75,
            "status": "completed"
        }


@patch("pathlib.Path.stat")
def test_get_index(mock_stat, logger, temp_file):
    """Test that file index is generated correctly."""
    # Mock the stat result to control modification time
    mock_stat.return_value.st_mtime = 1234567890.0

    index = logger._get_index(temp_file)
    assert index == (temp_file.name, 1234567890.0)


def test_read_existing_log(temp_log_file, temp_file):
    """Test that file states are correctly read from an existing json log."""
    # Manually write a row to the log file
    with open(temp_log_file, "w", newline='') as fd:
        json.dump(
            {
                "file_path": temp_file.name,
                "modification_time": temp_file.stat().st_mtime,
                "progress": 10,
                "status": "in-progress"
            },
            fd
        )
        fd.write("\n")
        json.dump(
            {
                "file_path": temp_file.name,
                "modification_time": temp_file.stat().st_mtime,
                "progress": 25,
                "status": "started"
            },
            fd
        )
        fd.write("\n")

    # Reload the logger to test reading from the log
    logger = FileProgressLogger(temp_log_file)

    # Verify the state is read correctly
    retrieved_state = logger.get_state(temp_file)
    assert retrieved_state is not None
    assert retrieved_state == {
        "progress": 25,
        "status": "started"
    }
