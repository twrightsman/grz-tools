import sys
from pathlib import Path

import pytest
from unittest.mock import patch

import grz_upload.cli


@pytest.fixture(scope='session')
def local_submission_dir(tmpdir_factory: pytest.TempdirFactory):
    """Create temporary folder for the session"""
    datadir = tmpdir_factory.mktemp('submission')
    return datadir


@pytest.fixture
def local_submission_dir_path(local_submission_dir) -> Path:
    return Path(local_submission_dir.strpath)


def test_prepare_submission(temp_metadata_file_path, temp_config_file_path, local_submission_dir_path):
    testargs = [
        "grz_upload/cli.py",
        "prepare-submission",
        "-c", temp_config_file_path,
        "--metafile", temp_metadata_file_path.name,
        "--submission_dir", local_submission_dir_path.name,
    ]
    with patch.object(sys, 'argv', testargs):
        grz_upload.cli.main()

    # TODO implement
    # assert that files have the correct structure:
    # /submission root directory
    # ├── metadata
    # │   └── metadata.json
    # └── files
    #     ├── 5M.fastq.gz.c4gh
    #     └── small_input_file.txt


def test_validate_submission(temp_metadata_file_path, temp_config_file_path):
    testargs = [
        "grz_upload/cli.py",
        "validate-submission",
        "-c", temp_config_file_path.name,
        "--submission_dir", temp_metadata_file_path.name,
    ]
    with patch.object(sys, 'argv', testargs):
        grz_upload.cli.main()

    # test if command has correctly checked for:
    # - mismatched md5sums
    # - all files existing


def test_submission(temp_metadata_file_path, temp_config_file_path):
    testargs = [
        "grz_upload/cli.py",
        "submit",
        "-c", temp_config_file_path.name,
        "--metafile", temp_metadata_file_path.name,
    ]
    with patch.object(sys, 'argv', testargs):
        grz_upload.cli.main()

    # TODO implement
    # check if upload to S3 bucket is working correctly
