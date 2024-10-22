"""Tests for the upload module"""

from pathlib import Path

import pytest
import yaml
from moto import mock_aws

from grz_cli.file_operations import calculate_sha256
from grz_cli.upload import S3BotoUploadWorker


@pytest.fixture(scope="module")
def temp_log_dir(tmpdir_factory: pytest.TempdirFactory):
    """Create temporary log folder for this pytest module"""
    datadir = tmpdir_factory.mktemp("logs")
    return datadir


@pytest.fixture
def temp_upload_log_file_path(temp_log_dir) -> Path:
    log_file = Path(temp_log_dir) / "progress_upload.cjson"
    return log_file


def download_file(remote_bucket, object_id, output_path):
    remote_bucket.download_file(object_id, output_path)


@mock_aws
def test_boto_upload(
    temp_config_file_path,
    remote_bucket,
    temp_small_file_path,
    temp_small_file_sha256sum,
    temp_fastq_file_path,
    temp_fastq_file_sha256sum,
    temp_upload_log_file_path,
    tmpdir_factory,
):
    # read S3 config
    with open(temp_config_file_path, encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    # create upload worker
    upload_worker = S3BotoUploadWorker(
        s3_settings=config, status_file_path=temp_upload_log_file_path
    )

    upload_worker.upload_file(temp_small_file_path, "small_test_file.txt")
    upload_worker.upload_file(temp_fastq_file_path, "large_test_file.fastq")

    # download files again
    local_tmpdir = tmpdir_factory.mktemp("redownload")
    local_tmpdir_path = Path(local_tmpdir.strpath)

    download_file(
        remote_bucket, "small_test_file.txt", local_tmpdir_path / "small_test_file.txt"
    )
    download_file(
        remote_bucket,
        "large_test_file.fastq",
        local_tmpdir_path / "large_test_file.fastq",
    )

    assert (
        calculate_sha256(local_tmpdir_path / "small_test_file.txt")
        == temp_small_file_sha256sum
    )
    assert (
        calculate_sha256(local_tmpdir_path / "large_test_file.fastq")
        == temp_fastq_file_sha256sum
    )
