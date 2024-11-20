"""Tests for the command line interface."""

import shutil
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner
from moto import mock_aws

import grz_cli.cli
from grz_cli.file_operations import calculate_sha256


@pytest.fixture(scope="session")
def working_dir(tmpdir_factory: pytest.TempdirFactory):
    """Create temporary folder for the session"""
    datadir = tmpdir_factory.mktemp("submission")
    return datadir


@pytest.fixture
def working_dir_path(working_dir) -> Path:
    return Path(working_dir.strpath)


def test_validate_submission(
    working_dir_path,
    temp_config_file_path,
):
    submission_dir = Path("tests/mock_files/submissions/valid_submission")

    shutil.copytree(
        submission_dir / "files", working_dir_path / "files", dirs_exist_ok=True
    )
    shutil.copytree(
        submission_dir / "metadata", working_dir_path / "metadata", dirs_exist_ok=True
    )

    testargs = [
        "validate",
        "--submission-dir",
        str(working_dir_path),
    ]

    runner = CliRunner()
    result = runner.invoke(grz_cli.cli.cli, testargs, catch_exceptions=False)

    # check if re-validation is skipped
    result = runner.invoke(grz_cli.cli.cli, testargs, catch_exceptions=False)

    # test if command has correctly checked for:
    # - mismatched md5sums
    # - all files existing

    assert result.exit_code == 0, result.output


def test_encrypt_decrypt_submission(
    working_dir_path,
    temp_config_file_path,
    # crypt4gh_grz_private_key_file_path,
    tmpdir_factory: pytest.TempdirFactory,
):
    submission_dir = Path("tests/mock_files/submissions/valid_submission")

    shutil.copytree(
        submission_dir / "files", working_dir_path / "files", dirs_exist_ok=True
    )
    shutil.copytree(
        submission_dir / "metadata", working_dir_path / "metadata", dirs_exist_ok=True
    )

    # first, encrypt the data
    testargs = [
        "encrypt",
        "--submission-dir",
        str(working_dir_path),
        "--config-file",
        temp_config_file_path,
    ]

    runner = CliRunner()
    result = runner.invoke(grz_cli.cli.cli, testargs, catch_exceptions=False)

    assert result.exit_code == 0, result.output

    # then, decrypt the data again
    testargs = [
        "decrypt",
        "--submission-dir",
        str(working_dir_path),
        "--config-file",
        temp_config_file_path,
    ]

    runner = CliRunner()
    result = runner.invoke(grz_cli.cli.cli, testargs, catch_exceptions=False)

    assert result.exit_code == 0, result.output

    # compare if the files are equal
    for file in [
        "aaaaaaaa00000000aaaaaaaa00000000_blood_normal.read1.fastq.gz",
        "aaaaaaaa00000000aaaaaaaa00000000_blood_normal.read2.fastq.gz",
        "aaaaaaaa00000000aaaaaaaa00000000_blood_tumor.read1.fastq.gz",
        "aaaaaaaa00000000aaaaaaaa00000000_blood_tumor.read2.fastq.gz",
        "aaaaaaaa00000000aaaaaaaa00000002_blood_normal.read1.fastq.gz",
        "aaaaaaaa00000000aaaaaaaa00000002_blood_normal.read2.fastq.gz",
    ]:
        expected_checksum = calculate_sha256(submission_dir / "files" / file)
        observed_checksum = calculate_sha256(working_dir_path / "files" / file)

        assert expected_checksum == observed_checksum


def test_decrypt_submission(working_dir_path, temp_config_file_path):
    submission_dir = Path("tests/mock_files/submissions/valid_submission")

    shutil.copytree(
        submission_dir / "encrypted_files",
        working_dir_path / "encrypted_files",
        dirs_exist_ok=True,
    )
    shutil.copytree(
        submission_dir / "metadata", working_dir_path / "metadata", dirs_exist_ok=True
    )

    testargs = [
        "decrypt",
        "--submission-dir",
        str(working_dir_path),
        "--config-file",
        temp_config_file_path,
    ]
    runner = CliRunner()
    result = runner.invoke(grz_cli.cli.cli, testargs, catch_exceptions=False)

    assert result.exit_code == 0, result.output

    # compare if the files are equal
    for file in [
        "aaaaaaaa00000000aaaaaaaa00000000_blood_normal.read1.fastq.gz",
        "aaaaaaaa00000000aaaaaaaa00000000_blood_normal.read2.fastq.gz",
        "aaaaaaaa00000000aaaaaaaa00000000_blood_tumor.read1.fastq.gz",
        "aaaaaaaa00000000aaaaaaaa00000000_blood_tumor.read2.fastq.gz",
        "aaaaaaaa00000000aaaaaaaa00000002_blood_normal.read1.fastq.gz",
        "aaaaaaaa00000000aaaaaaaa00000002_blood_normal.read2.fastq.gz",
    ]:
        expected_checksum = calculate_sha256(submission_dir / "files" / file)
        observed_checksum = calculate_sha256(working_dir_path / "files" / file)

        assert expected_checksum == observed_checksum


@mock_aws
def test_upload_submission(working_dir_path, temp_config_file_path, remote_bucket):
    submission_dir = Path("tests/mock_files/submissions/valid_submission")

    shutil.copytree(
        submission_dir / "files", working_dir_path / "files", dirs_exist_ok=True
    )
    shutil.copytree(
        submission_dir / "metadata", working_dir_path / "metadata", dirs_exist_ok=True
    )

    testargs = [
        "upload",
        "--submission-dir",
        str(working_dir_path),
        "--config-file",
        temp_config_file_path,
    ]
    runner = CliRunner()

    # somehow, mock_aws does not catch the boto3 client properly here,
    # so we mock the boto3 client manually
    with mock.patch("botocore.client.BaseClient._make_api_call"):
        result = runner.invoke(grz_cli.cli.cli, testargs, catch_exceptions=False)

    # TODO implement
    # check if upload to S3 bucket is working correctly

    assert result.exit_code == 0, result.output
