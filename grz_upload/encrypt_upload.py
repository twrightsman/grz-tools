import hashlib
import boto3
import csv
import yaml
import click
import os
from nacl.public import PrivateKey
from nacl.bindings import crypto_aead_chacha20poly1305_ietf_encrypt
from crypt4gh.keys import get_public_key
import crypt4gh.header as header
from typing import Dict, List, Tuple


# Constants
MULTIPART_CHUNK_SIZE = 50 * 1024 * 1024  # 50 MB
# Crypt4gh constants
VERSION = 1
SEGMENT_SIZE = 65536


def log_progress(log_file: str, file_path: str, message: str) -> None:
    with open(log_file, 'a') as log:
        log.write(f"{file_path}: {message}\n")


def read_progress(log_file: str) -> Dict[str, str]:
    progress = {}
    if os.path.exists(log_file):
        with open(log_file, 'r') as log:
            for line in log:
                file_path, status = line.strip().split(": ", 1)
                progress[file_path] = status
    return progress


def validate_metadata(metadata_file_path: str) -> int:
    """Validate the fields exist and filepaths are reachable."""
    # what fields to check
    required_fields = ['File id', 'File Location']
    with open(metadata_file_path, 'r') as csvfile:
        # make sure all values are filled
        reader = csv.DictReader(csvfile, delimiter=',')
        # Check for required fields
        for field in required_fields:
            if field not in reader.fieldnames:
                raise ValueError(f"Metadata file is missing required field: {field}")
        # check file Proposal_encrypted_chunks_header
        files_count = 0
        missing_files = []
        for row in reader:
            files_count += 1
            file_location = row['File Location']
            if not os.path.exists(file_location):
                missing_files.append(file_location)
        if missing_files:
            raise FileNotFoundError(f"The following files are missing: {', '.join(missing_files)}")
    return files_count


def print_summary(metadata_file_path: str, log_file: str) -> None:
    progress = read_progress(log_file)
    total_files = validate_metadata(metadata_file_path)
    uploaded_files = sum(1 for status in progress.values() if status == 'finished')
    failed_files = sum(1 for status in progress.values() if 'failed' in status)
    waiting_files = total_files - uploaded_files - failed_files

    print(f"Total files: {total_files}")
    print(f"Uploaded files: {uploaded_files}")
    print(f"Failed files: {failed_files}")
    print(f"Waiting files: {waiting_files}")


def calculate_md5(file_path: str, chunk_size: int = 4096) -> str:
    """Calculate the MD5 hash of a file in chunks."""
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def prepare_c4gh_keys(public_key: str) -> Tuple[Tuple[int, bytes, bytes]]:
    """Prepare the key format c4gh needs, while it can contain
    multiple keys for multiple recipients, in our use case there is
    a single recipient"""
    sk = PrivateKey.generate()
    seckey = bytes(sk)
    keys = ((0, seckey, get_public_key(public_key)), )
    return keys


def prepare_header(keys: Tuple[Tuple[int, bytes, bytes]]) -> Tuple[bytes, bytes, Tuple[Tuple[int, bytes, bytes]]]:
    """Prepare header separately to be able to use multiupload"""
    encryption_method = 0  # only choice for this version
    session_key = os.urandom(32)  # we use one session key for all blocks
    # Output the header
    header_content = header.make_packet_data_enc(encryption_method, session_key)
    header_packets = header.encrypt(header_content, keys)
    header_bytes = header.serialize(header_packets)
    return (header_bytes, session_key, keys)


def encrypt_segment(data: bytes, key: bytes) -> bytes:
    """Encrypt 64kb block with crypt4gh"""
    nonce = os.urandom(12)
    encrypted_data = crypto_aead_chacha20poly1305_ietf_encrypt(data, None, nonce, key)
    return nonce + encrypted_data


def encrypt_part(byte_string: bytes, session_key: bytes) -> bytes:
    """Encrypt incoming chunk, using session_key"""
    data_size = len(byte_string)
    enc_data = b''
    position = 0
    while True:
        data_block = b''
        # Determine how much data to read
        segment_len = min(SEGMENT_SIZE, data_size - position)
        if segment_len == 0:  # No more data to read
            break
        # Read the segment from the byte string
        data_block = byte_string[position:position + segment_len]
        # Update the position
        position += segment_len
        # Process the data in `segment`
        enc_data += encrypt_segment(data_block, session_key)
    return enc_data


def stream_encrypt_and_upload(file_location: str, 
                              file_id: str, 
                              keys: Tuple[Tuple[int, bytes, bytes]],
                              s3_client: boto3.client, 
                              s3_bucket: str, 
                              log_file: str
                              ) -> Tuple[str, str]:
    """Encrypt and upload the file in chunks, properly handling the Crypt4GH header."""
    # Generate the header

    multipart_upload = s3_client.create_multipart_upload(Bucket=s3_bucket, Key=file_id)
    upload_id = multipart_upload['UploadId']
    parts = []
    part_number = 1

    try:
        # Initialize MD5 calculations
        original_md5 = hashlib.md5()
        encrypted_md5 = hashlib.md5()

        with open(file_location, 'rb') as infile:
            # prepare header
            header_info = prepare_header(keys)
            # we will prepend the header to the first chunk
            first_chunk_read = False

            # Process and encrypt the file in chunks
            while chunk := infile.read(MULTIPART_CHUNK_SIZE):
                original_md5.update(chunk)
                encrypted_chunk = encrypt_part(chunk, header_info[1])
                # add header to the first chunk
                if not first_chunk_read:
                    encrypted_chunk = header_info[0] + encrypted_chunk
                    first_chunk_read = True
                encrypted_md5.update(encrypted_chunk)

                # Upload each encrypted chunk
                log_progress(log_file, file_location, f'Uploading part {part_number}')
                part = s3_client.upload_part(
                    Bucket=s3_bucket,
                    Key=file_id,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=encrypted_chunk
                )
                parts.append({
                    'PartNumber': part_number,
                    'ETag': part['ETag']
                })
                part_number += 1

        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=s3_bucket,
            Key=file_id,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        log_progress(log_file, file_location, 'finished')

        return original_md5.hexdigest(), encrypted_md5.hexdigest()

    except Exception as e:
        log_progress(log_file, file_location, f'part{part_number} failed: {str(e)}')
        s3_client.abort_multipart_upload(Bucket=s3_bucket, Key=file_id, UploadId=upload_id)
        raise e


def encrypt_and_upload_files(
        metadata_file_path: str,
        public_key_path: str,
        s3_client: boto3.client,
        s3_bucket: str,
        log_file: str
        ) -> None:
    print_summary(metadata_file_path, log_file)

    # prepare symetric key pack for Crypt4GH
    keys = prepare_c4gh_keys(public_key_path)

    # Read the metadata file and process each file
    with open(metadata_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        fieldnames = reader.fieldnames + ['original_md5', 'encrypted_md5', 'upload_status']

        # Create a temporary file to store progress
        temp_metadata_file_path = metadata_file_path + '.tmp'
        with open(temp_metadata_file_path, 'w', newline='') as temp_csvfile:
            writer = csv.DictWriter(temp_csvfile, fieldnames=fieldnames, delimiter=',')
            writer.writeheader()

            progress = read_progress(log_file)
            # Reading files one by one from the metadata file
            for row in reader:
                file_id = row['File id']
                file_location = row['File Location']

                # Skip files that are already marked as completed
                if progress.get(file_location) == 'finished':
                    continue

                log_progress(log_file, file_location, 'waiting')

                try:
                    # Encrypt and upload the file in chunks
                    original_md5, encrypted_md5 = stream_encrypt_and_upload(
                        file_location, file_id, keys,
                        s3_client, s3_bucket, log_file
                    )

                    # Add MD5 checksums and upload status to the row
                    row['original_md5'] = original_md5
                    row['encrypted_md5'] = encrypted_md5
                    row['upload_status'] = 'success'

                except Exception as e:
                    row['upload_status'] = f'failed: {str(e)}'
                    print(f"Error: {str(e)}")

                # Write the updated row to the temporary metadata file
                writer.writerow(row)

    # Replace the original metadata file with the updated one
    os.replace(temp_metadata_file_path, metadata_file_path)


@click.command()
@click.option('--config', default='config.yaml', help='Path to the configuration file.')
def main(config: str) -> None:
    # Load configuration
    with open(config, 'r') as config_file:
        config = yaml.safe_load(config_file)

    # Initialize S3 client for uploading
    s3_client = boto3.client('s3',
                             endpoint_url=config['s3_url'],
                             aws_access_key_id=config['s3_access_key'],
                             aws_secret_access_key=config['s3_secret'])

    encrypt_and_upload_files(
        config['metadata_file_path'],
        config['public_key_path'],
        s3_client,
        config['s3_bucket'],
        config.get('log_file', 'upload_progress.log')
    )


if __name__ == "__main__":
    main()
