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

def encrypt_and_upload_files(metadata_file_path, public_key_path, s3_bucket, log_file):
    # Load the public key for encryption
    with open(public_key_path, 'rb') as key_file:
        public_key = get_public_key(key_file)

    # Initialize S3 client for uploading
    s3_client = boto3.client('s3')

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
                file_path = row['file_path']
                s3_key = row['s3_key']

                # Skip files that are already marked as completed
                if progress.get(file_path) == 'finished':
                    continue

                log_progress(log_file, file_path, 'waiting')

                # Initialize MD5 hash objects for original and encrypted files
                original_md5_hash = hashlib.md5()
                encrypted_md5_hash = hashlib.md5()

                # Calculate MD5 for the original file
                try:
                    with open(file_path, 'rb') as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            original_md5_hash.update(chunk)

                    # Open the file again to read in binary mode for encryption and upload
                    with open(file_path, 'rb') as f:
                        # Define a generator for encrypted stream
                        def encrypted_stream():
                            yield from crypt4gh.lib.encrypt([public_key], f)

                        # Define a generator to update MD5 hash and yield encrypted chunks
                        def md5_and_yield(stream):
                            for chunk in stream:
                                encrypted_md5_hash.update(chunk)
                                yield chunk

                        # Start multipart upload
                        multipart_upload = s3_client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)
                        upload_id = multipart_upload['UploadId']
                        parts = []
                        part_number = 1

                        try:
                            # Upload file in chunks
                            for part in md5_and_yield(encrypted_stream()):
                                if len(part) < MULTIPART_CHUNK_SIZE:
                                    log_progress(log_file, file_path, f'part{part_number} being uploaded')
                                    parts.append({
                                        'PartNumber': part_number,
                                        'ETag': s3_client.upload_part(
                                            Bucket=s3_bucket,
                                            Key=s3_key,
                                            PartNumber=part_number,
                                            UploadId=upload_id,
                                            Body=part
                                        )['ETag']
                                    })
                                    part_number += 1

                            # Complete multipart upload
                            s3_client.complete_multipart_upload(
                                Bucket=s3_bucket,
                                Key=s3_key,
                                UploadId=upload_id,
                                MultipartUpload={'Parts': parts}
                            )
                            log_progress(log_file, file_path, 'finished')

                        except Exception as e:
                            # Abort multipart upload in case of error
                            log_progress(log_file, file_path, f'part{part_number} failed: {str(e)}')
                            s3_client.abort_multipart_upload(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id)
                            raise e

                    # Add MD5 checksums and upload status to the row
                    row['original_md5'] = original_md5_hash.hexdigest()
                    row['encrypted_md5'] = encrypted_md5_hash.hexdigest()
                    row['upload_status'] = 'success'

                except Exception as e:
                    row['upload_status'] = f'failed: {str(e)}'

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
        config['s3_bucket'],
        config['s3_access_key'],
        config['s3_secret'],
        config.get('log_file', 'upload_progress.log')
    )

if __name__ == "__main__":
    main()