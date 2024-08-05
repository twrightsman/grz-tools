import hashlib
import boto3
import crypt4gh.lib
from crypt4gh.keys import get_public_key
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

def calculate_md5(file_path):
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def encrypt_file(input_path, output_path, public_key):
    with open(input_path, 'rb') as infile, open(output_path, 'wb') as outfile:
        crypt4gh.lib.encrypt([public_key], infile, outfile)

def encrypt_and_upload_files(metadata_file_path, public_key_path,
                             s3_url, s3_access_key, s3_secret, s3_bucket, log_file):
    # Validate metadata and print summary
    print_summary(metadata_file_path, log_file)

    # Load the public key for encryption
    public_key = get_public_key(public_key_path)

    # Initialize S3 client for uploading
    s3_client = boto3.client('s3',
                             endpoint_url=s3_url,
                             aws_access_key_id=s3_access_key,
                             aws_secret_access_key=s3_secret,
                      )

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
                temp_encrypted_file = f"{file_id}_temp.enc"

                # Skip files that are already marked as completed
                if progress.get(file_location) == 'finished':
                    continue

                log_progress(log_file, file_location, 'waiting')

                try:
                    # Calculate MD5 for the original file
                    original_md5 = calculate_md5(file_location)

                    # Encrypt the file and save to a temporary location
                    encrypt_file(file_location, temp_encrypted_file, public_key)

                    # Calculate MD5 for the encrypted file
                    encrypted_md5 = calculate_md5(temp_encrypted_file)

                    # Start multipart upload
                    multipart_upload = s3_client.create_multipart_upload(Bucket=s3_bucket, Key=file_id)
                    upload_id = multipart_upload['UploadId']
                    parts = []
                    part_number = 1

                    try:
                        # Upload file in chunks
                        with open(temp_encrypted_file, 'rb') as f:
                            for chunk in iter(lambda: f.read(MULTIPART_CHUNK_SIZE), b""):
                                log_progress(log_file, file_location, f'part{part_number} being uploaded')
                                parts.append({
                                    'PartNumber': part_number,
                                    'ETag': s3_client.upload_part(
                                        Bucket=s3_bucket,
                                        Key=file_id,
                                        PartNumber=part_number,
                                        UploadId=upload_id,
                                        Body=chunk
                                    )['ETag']
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

                    except Exception as e:
                        # Abort multipart upload in case of error
                        log_progress(log_file, file_location, f'part{part_number} failed: {str(e)}')
                        s3_client.abort_multipart_upload(Bucket=s3_bucket, Key=file_id, UploadId=upload_id)
                        raise e

                    # Add MD5 checksums and upload status to the row
                    row['original_md5'] = original_md5
                    row['encrypted_md5'] = encrypted_md5
                    row['upload_status'] = 'success'

                except Exception as e:
                    print(s3_url)
                    print(str(e))

                    row['upload_status'] = f'failed: {str(e)}'
                    print(f"Error: {str(e)}")

                # Write the updated row to the temporary metadata file
                writer.writerow(row)

                # Delete the temporary encrypted file
                if os.path.exists(temp_encrypted_file):
                    os.remove(temp_encrypted_file)

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