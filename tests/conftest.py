import pytest
import tempfile
import os
import boto3
from unittest.mock import patch, MagicMock
import yaml
from unittest import mock


@pytest.fixture
def temp_log_file(tmpdir):
    log_file = tmpdir.join("log.txt")
    return str(log_file)


@pytest.fixture
def temp_metadata_file(tmpdir):
    metadata_file = tmpdir.join("metadata.csv")
    metadata_file.write("File id,File Location\n1,/path/to/file\n")
    return str(metadata_file)


@pytest.fixture
def temp_data_file(tmpdir):
    data_file = tmpdir.join("data.txt")
    data_file.write("This is a test.")
    return str(data_file)


@pytest.fixture
def mock_s3_client():
    with mock.patch('boto3.client') as mock_client:
        yield mock_client


# mocking with moto
#
# import pathlib
# from tempfile import NamedTemporaryFile
#
# import boto3
# import moto
# import pytest
# from botocore.exceptions import ClientError
#
# from os_bucket import OSBucket
#
#
# @pytest.fixture
# def empty_bucket():
#     moto_fake = moto.mock_s3()
#     try:
#         moto_fake.start()
#         conn = boto3.resource('s3')
#         conn.create_bucket(Bucket="OS_BUCKET")  # or the name of the bucket you use
#         yield conn
#     finally:
#         moto_fake.stop()
#
#
# def test_download_non_existing_path(empty_bucket):
#     os_bucket = OSBucket()
#     os_bucket.initBucket()
#     with pytest.raises(ClientError) as e:
#         os_bucket.download_file("bad_path", "bad_file")
#     assert "Not Found" in str(e)
#
#
# def test_upload_and_download(empty_bucket):
#     os_bucket = OSBucket()
#     os_bucket.initBucket()
#     with NamedTemporaryFile() as tmp:
#         tmp.write(b'Hi')
#         file_name = pathlib.Path(tmp.name).name
#
#         os_bucket.upload_file(tmp.name, file_name)
#         os_bucket.download_file("/" + file_name, file_name)  # this might indicate a bug in the implementation
