import boto3
import yaml
from moto import mock_aws

from grz_upload.upload import S3UploadWorker


def create_bucket(bucket_name, config):
    # Initialize S3 client for uploading
    temp_s3_client = boto3.client(
        's3',
        endpoint_url=None,
        aws_access_key_id=config['s3_access_key'],
        aws_secret_access_key=config['s3_secret']
    )
    # create bucket
    temp_s3_client.create_bucket(Bucket=bucket_name)


@mock_aws
def test_upload(
        temp_small_input_file,
        temp_small_input_file_md5sum,
        temp_fastq_gz_file,
        temp_fastq_gz_file_md5sum,
        temp_crypt4gh_public_key_file,
        temp_config_file,
        tmp_path_factory
):
    bucket_name = 'test_upload'

    # read S3 config
    with open(temp_config_file, encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)
    # change bucket name
    config["s3_bucket"] = bucket_name
    # create bucket
    create_bucket(bucket_name, config)

    # create upload worker
    upload_worker = S3UploadWorker(
        s3_dict=config,
        status_file_path=None,
        pubkey_grz_file=temp_crypt4gh_public_key_file
    )

    md5sums = upload_worker.upload_files({
        "small_test_file.txt": temp_small_input_file,
        "large_test_file.fastq.gz": temp_fastq_gz_file,
    })

    assert md5sums["small_test_file.txt"] == temp_small_input_file_md5sum
    assert md5sums["large_test_file.fastq.gz"] == temp_fastq_gz_file_md5sum

    md5sums = upload_worker.encrypt_upload_files({
        "small_test_file.txt.c4gh": temp_small_input_file,
        "large_test_file.fastq.gz.c4gh": temp_fastq_gz_file,
    })

    # TODO: also test encrypted file md5 sums
    assert md5sums["small_test_file.txt.c4gh"][0] == temp_small_input_file_md5sum
    assert md5sums["large_test_file.fastq.gz.c4gh"][0] == temp_fastq_gz_file_md5sum
