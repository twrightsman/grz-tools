import hashlib
import boto3
import io
from crypt4gh.lib import encrypt
from crypt4gh.cli import build_recipients
import csv
import os
import yaml
import click

# Constants
MULTIPART_CHUNK_SIZE = 50 * 1024 * 1024  # 50 MB

def log_progress(log_file, file_path, message):
    with open(log_file, 'a') as log:
        log.write(f"{file_path}: {message}\n")

def read_progress(log_file):
    progress = {}
    if os.path.exists(log_file):
        with open(log_file, 'r') as log:
            for line in log:
                file_path, status = line.strip().split(": ", 1)
                progress[file_path] = status
    return progress

def validate_metadata(metadata_file_path):
    required_fields = ['File id', 'File Location']
    with open(metadata_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for field in required_fields:
            if field not in reader.fieldnames:
                raise ValueError(f"Metadata file is missing required field: {field}")
        files_count = sum(1 for _ in reader)
    return files_count

def print_summary(metadata_file_path, log_file):
    progress = read_progress(log_file)
    total_files = validate_metadata(metadata_file_path)
    uploaded_files = sum(1 for status in progress.values() if status == 'finished')
    failed_files = sum(1 for status in progress.values() if 'failed' in status)
    waiting_files = total_files - uploaded_files - failed_files

    print(f"Total files: {total_files}")
    print(f"Uploaded files: {uploaded_files}")
    print(f"Failed files: {failed_files}")
    print(f"Waiting files: {waiting_files}")

def calculate_md5(file_path, chunk_size=4096):
    """Calculate the MD5 hash of a file in chunks."""
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def stream_encrypt_and_upload(file_location, file_id, public_key_path, s3_client, s3_bucket, log_file):
    """Encrypt and upload the file in chunks."""
    # Start multipart upload
    multipart_upload = s3_client.create_multipart_upload(Bucket=s3_bucket, Key=file_id)
    upload_id = multipart_upload['UploadId']
    parts = []
    part_number = 1

    header = None
    try:
        with open(file_location, 'rb') as infile:
            # Calculate MD5 for the original file while streaming
            original_md5 = hashlib.md5()
            encrypted_md5 = hashlib.md5()

            while True:
                chunk = infile.read(MULTIPART_CHUNK_SIZE)
                if not chunk:
                    break
                original_md5.update(chunk)

                # Encrypt the chunk
                if header is None:
                    # Generate the header if it's the first chunk
                    header = io.BytesIO()
                    encrypt([public_key], io.BytesIO(chunk), header)
                    encrypted_chunk = header.getvalue()
                else:
                    encrypted_chunk_stream = io.BytesIO()
                    encrypt([public_key], io.BytesIO(chunk), encrypted_chunk_stream)
                    encrypted_chunk = encrypted_chunk_stream.getvalue()

                encrypted_md5.update(encrypted_chunk)

                # Upload the encrypted chunk
                log_progress(log_file, file_location, f'part{part_number} being uploaded')
                part = s3_client.upload_part(
                    Bucket=s3_bucket,
                    Key=file_id,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=io.BytesIO(encrypted_chunk)
                )
                parts.append({
                    'PartNumber': part_number,
                    'ETag': part['ETag']
                })
                part_number += 1

        # Complete multipart upload
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

def encrypt_and_upload_files(metadata_file_path, public_key_path,
                             s3_url, s3_access_key, s3_secret, s3_bucket, log_file):
    # Validate metadata and print summary
    print_summary(metadata_file_path, log_file)

    # Initialize S3 client for uploading
    s3_client = boto3.client('s3',
                             endpoint_url=s3_url,
                             aws_access_key_id=s3_access_key,
                             aws_secret_access_key=s3_secret)

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
                        file_location, file_id, public_key_path, s3_client, s3_bucket, log_file
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
def main(config):
    # Load configuration

    with open(config, 'r') as config_file:
        config = yaml.safe_load(config_file)

    encrypt_and_upload_files(
        config['metadata_file_path'],
        config['public_key_path'],
        config['s3_url'],
        config['s3_access_key'],
        config['s3_secret'],
        config['s3_bucket'],
        config.get('log_file', 'upload_progress.log')
    )

if __name__ == "__main__":
    main()
