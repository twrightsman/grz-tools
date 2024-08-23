import os  # noqa: D100
import shutil
from unittest import mock

import pytest
from grz_upload.encrypt_upload import (
    calculate_md5,
    encrypt_part,
    encrypt_segment,
    log_progress,
    main,
    prepare_c4gh_keys,
    prepare_header,
    print_summary,
    read_progress,
    stream_encrypt_and_upload,
    validate_metadata,
)
from moto import mock_aws
from nacl.public import PrivateKey

# create fixtures from the mock files stored under tests
# some files get modified, use temdir for storing these temporary copies

def test_config_file(temp_config_file):
    print(temp_config_file)

def test_log_progress(temp_log_file):
    """Test for log_progress function"""
    log_progress(temp_log_file, "test_path", "test_message")
    with open(temp_log_file, encoding='utf8') as log:
        assert "test_path: test_message" in log.read()


def test_read_progress(temp_log_file):
    """Test for read_progress function"""
    log_progress(temp_log_file, "test_path", "test_message")
    progress = read_progress(temp_log_file)
    assert progress["test_path"] == "test_message"


def test_validate_metadata(temp_metadata_file, temp_input_file):
    """Assert there is 1 file in the metadata and fields are in place"""
    result = validate_metadata(temp_metadata_file)
    assert result==1 

# def test_print_summary(temp_metadata_file, temp_log_file):
#     print_summary(temp_metadata_file, temp_log_file)
#     # Add your assertions based on expected summary output


def test_calculate_md5(temp_input_file):
    md5 = calculate_md5(temp_input_file)
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


def test_cleanup_time(datadir):
    """Delete the temporary files and folders."""
    # Check if the folder exists
    if not os.path.exists(datadir):
        print(f"Folder '{datadir}' does not exist.")
        return

    # Loop through all the files in the folder
    for filename in os.listdir(datadir):
        file_path = os.path.join(datadir, filename)
        
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # Remove the file or link
                print(f"Deleted file: {file_path}")
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # Remove a directory and all its contents
                print(f"Deleted directory: {file_path}")
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

    # After deleting all files, delete the empty folder
    try:
        os.rmdir(datadir)
        print(f"Deleted folder: {datadir}")
    except Exception as e:
        print(f"Failed to delete folder {datadir}. Reason: {e}")