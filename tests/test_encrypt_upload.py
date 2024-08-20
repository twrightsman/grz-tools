import pytest
from unittest import mock
import os
from nacl.public import PrivateKey
from grz_upload.encrypt_upload import (
    log_progress, read_progress, validate_metadata, print_summary,
    calculate_md5, prepare_c4gh_keys, prepare_header, encrypt_segment,
    encrypt_part, stream_encrypt_and_upload, main
)


def test_log_progress(temp_log_file):
    log_progress(temp_log_file, "test_path", "test_message")
    with open(temp_log_file, 'r') as log:
        assert "test_path: test_message" in log.read()


def test_read_progress(temp_log_file):
    log_progress(temp_log_file, "test_path", "test_message")
    progress = read_progress(temp_log_file)
    assert progress["test_path"] == "test_message"


# def test_validate_metadata(temp_metadata_file):
#     result = validate_metadata(temp_metadata_file)
#     assert result is None  # Assuming the function returns None on success


# def test_print_summary(temp_metadata_file, temp_log_file):
#     print_summary(temp_metadata_file, temp_log_file)
#     # Add your assertions based on expected summary output


def test_calculate_md5(temp_data_file):
    md5 = calculate_md5(temp_data_file)
    assert isinstance(md5, str)
    assert len(md5) == 32  # MD5 hash is 32 characters long


# def test_prepare_c4gh_keys():
#     public_key = PrivateKey.generate().public_key
#     keys = prepare_c4gh_keys(public_key)
#     assert isinstance(keys, dict)
#     assert "session_key" in keys
#     assert "public_key" in keys
#
#
# def test_prepare_header():
#     keys = {'session_key': b'test_key', 'public_key': b'test_public_key'}
#     header = prepare_header(keys)
#     assert isinstance(header, bytes)
#
#
# def test_encrypt_segment():
#     data = b"test data"
#     key = b"test_key_32_bytes_long____"
#     encrypted_data = encrypt_segment(data, key)
#     assert isinstance(encrypted_data, bytes)
#     assert len(encrypted_data) > len(data)
#
#
# def test_encrypt_part():
#     byte_string = b"test bytes"
#     session_key = b"test_key_32_bytes_long____"
#     encrypted_part = encrypt_part(byte_string, session_key)
#     assert isinstance(encrypted_part, bytes)
#
#
# def test_stream_encrypt_and_upload(temp_data_file, mock_s3_client, temp_log_file):
#     keys = {'session_key': b'test_key', 'public_key': b'test_public_key'}
#     stream_encrypt_and_upload(temp_data_file, "file_id", keys, mock_s3_client, "test_bucket", temp_log_file)
#     # Add assertions based on expected interactions with mock_s3_client
#
#
# def test_main():
#     config = {"key": "value"}  # Mock your config as needed
#     with mock.patch("grz_upload.encrypt_upload.stream_encrypt_and_upload") as mock_upload:
#         main(config)
#         mock_upload.assert_called_once()
