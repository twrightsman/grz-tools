import hashlib  # noqa: D100
import os
import shutil
import subprocess
from math import ceil

import boto3
import yaml
from grz_upload.encrypt_upload import (
    SEGMENT_SIZE,
    calculate_md5,
    encrypt_part,
    encrypt_segment,
    log_progress,
    prepare_c4gh_keys,
    prepare_header,
    print_summary,
    read_progress,
    stream_encrypt_and_upload,
    validate_metadata,
)
from moto import mock_aws

# create fixtures from the mock files stored under tests
# some files get modified, use temdir for storing these temporary copies


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


def test_print_summary(temp_metadata_file, temp_log_file, capsys):
    expected_message = """Total files: 1\nUploaded files: 0\nFailed files: 0\nWaiting files: 1\n"""
    print_summary(temp_metadata_file, temp_log_file)
    captured = capsys.readouterr()
    assert captured.out == expected_message
    # Add your assertions based on expected summary output


def test_calculate_md5(temp_input_file):
    md5 = calculate_md5(temp_input_file)
    assert isinstance(md5, str)
    assert len(md5) == 32  # MD5 hash is 32 characters long
    assert md5 == "710781ec9efd25b87bfbf8d6cf4030e9"


def test_prepare_c4gh_keys(temp_public_key_file):
    keys = prepare_c4gh_keys(temp_public_key_file)
    # single key in tupple
    assert len(keys) == 1
    # key method is set to 0
    assert keys[0][0] == 0
    # private key is generated
    assert len(keys[0][1]) == 32


def test_prepare_header(temp_c4gh_keys):
    header_pack = prepare_header(temp_c4gh_keys)
    assert len(header_pack) == 3
    # assert header size
    assert len(header_pack[0]) == 124
    # size of session key
    assert len(header_pack[1]) == 32
    # pass back the c4gh keys
    assert header_pack[2] == temp_c4gh_keys


def test_encrypt_segment(temp_c4gh_header):
    data = b"test segment content"
    key = temp_c4gh_header[1]
    encrypted_data = encrypt_segment(data, key)
    assert isinstance(encrypted_data, bytes)
    # size is larger by 12 nonce and 16 mac(?)
    assert len(encrypted_data) == len(data) + 12 + 16 


def test_encrypt_part(temp_input_file, temp_c4gh_header):
    with open(temp_input_file, 'rb') as infile:
        byte_string = infile.read()
        session_key = temp_c4gh_header[1]
        encrypted_part = encrypt_part(byte_string, session_key)
    print(len(encrypted_part))
    file_size = len(byte_string)
    segment_no = ceil(file_size/SEGMENT_SIZE)
    encrypted_part_size = file_size + segment_no * (12 + 16)
    assert encrypted_part_size == len(encrypted_part)

@mock_aws
def test_stream_encrypt_and_upload(temp_large_input_file,
                                   temp_c4gh_keys,
                                   temp_config_file,
                                   temp_log_file,
                                   datadir,
                                   temp_private_key_file):
    bucket_name = 'temp_bucket'
    file_id = 'temp_id'
    # create client
    with open(temp_config_file, encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    # Initialize S3 client for uploading
    temp_s3_client = boto3.client('s3',
                             endpoint_url=None,
                             aws_access_key_id=config['s3_access_key'],
                             aws_secret_access_key=config['s3_secret'])
    
    # create bucket
    temp_s3_client.create_bucket(Bucket=bucket_name)
    # set chunks to 5MB
    multipart_chunk_size = 1024 * 1024 * 5
    # encrypt and upload
    input_md5_st, enc_md5_st = stream_encrypt_and_upload(file_location=temp_large_input_file,
                              file_id=file_id,
                              keys=temp_c4gh_keys,
                              s3_client=temp_s3_client,
                              s3_bucket=bucket_name,
                              log_file=temp_log_file,
                              multipart_chunk_size=multipart_chunk_size
                              )
    temp_enc_file = datadir.join('temp_enc_file.txt.c4gh')
    # download encrypted file
    temp_s3_client.download_file(bucket_name, file_id, temp_enc_file)
    # compare md5s
    input_md5 = hashlib.md5(open(temp_large_input_file,'rb').read()).hexdigest()
    enc_md5 = hashlib.md5(open(temp_enc_file,'rb').read()).hexdigest()
    assert input_md5 == input_md5_st
    assert enc_md5 == enc_md5_st
    # Decrypt file and make sure content is correct
    # Use subprocess - crypt4gh arguments < > are problematic for passing them directly
    # Open the input and output files
    temp_decr_file = datadir.join('temp_denc_file.txt')
    with open(temp_enc_file, 'rb') as input_file, open(temp_decr_file, 'wb') as output_file:
        command = ["crypt4gh", "decrypt", "--sk", temp_private_key_file]
        # Run the command with input and output redirection
        subprocess.run(command, stdin=input_file, stdout=output_file)
    
    denc_md5 = enc_md5 = hashlib.md5(open(temp_decr_file,'rb').read()).hexdigest()
    assert denc_md5 == enc_md5


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