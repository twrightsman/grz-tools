import hashlib
import boto3
import crypt4gh.lib
from crypt4gh.keys import get_public_key
import csv
import os
import yaml
import click

def encrypt_and_upload_files(metadata_file_path, public_key_path, s3_bucket):
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
            
            for row in reader:
                file_path = row['file_path']
                s3_key = row['s3_key']

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

                        # Upload the file to S3 in chunks
                        s3_client.upload_fileobj(md5_and_yield(encrypted_stream()), s3_bucket, s3_key)
                    
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
        config['s3_bucket']
    )

if __name__ == "__main__":
    main()
