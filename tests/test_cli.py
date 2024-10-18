from pathlib import Path

import pytest

import grz_upload.cli


@pytest.fixture(scope="session")
def local_submission_dir(tmpdir_factory: pytest.TempdirFactory):
    """Create temporary folder for the session"""
    datadir = tmpdir_factory.mktemp("submission")
    return datadir


@pytest.fixture
def local_submission_dir_path(local_submission_dir) -> Path:
    return Path(local_submission_dir.strpath)


def test_validate_submission(local_submission_dir_path, temp_config_file_path):
    testargs = [
        "validate",
        "--submission_dir",
        str(local_submission_dir_path),
    ]

    from click.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(grz_upload.cli.cli, testargs, catch_exceptions=False)

    # test if command has correctly checked for:
    # - mismatched md5sums
    # - all files existing

    assert result.exit_code == 0, result.output


def test_encrypt_submission(local_submission_dir_path, temp_config_file_path):
    testargs = [
        "encrypt",
        "--submission_dir",
        str(local_submission_dir_path),
    ]
    from click.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(grz_upload.cli.cli, testargs, catch_exceptions=False)

    # TODO implement
    # check if upload to S3 bucket is working correctly

    assert result.exit_code == 0, result.output


def test_upload_submission(local_submission_dir_path, temp_config_file_path):
    testargs = [
        "upload",
        "--submission_dir",
        str(local_submission_dir_path),
    ]
    from click.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(grz_upload.cli.cli, testargs, catch_exceptions=False)

    # TODO implement
    # check if upload to S3 bucket is working correctly

    assert result.exit_code == 0, result.output
