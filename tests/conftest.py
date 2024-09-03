import json
import os  # noqa: D100
from shutil import copyfile

import numpy as np
import pytest

from grz_upload.file_operations import Crypt4GH

config_path = "tests/mock_files/mock_config.yaml"
small_input_path = "tests/mock_files/mock_small_input_file.txt"
metadata_path = "tests/mock_files/example_metadata.json"
private_key_path = "tests/mock_files/mock_private_key.sec"
public_key_path = "tests/mock_files/mock_public_key.pub"


@pytest.fixture(scope='session')
def data_dir(tmpdir_factory):
    """Create temporary folder for the session"""
    datadir = tmpdir_factory.mktemp('data')
    return datadir


def copy_file_to_tempdir(input_path, datadir):
    filename = os.path.basename(input_path)
    target = datadir.join(filename)
    copyfile(input_path, target)
    return target


@pytest.fixture
def temp_log_file(data_dir):
    log_file = data_dir.join("log.txt")
    return str(log_file)


@pytest.fixture
def temp_small_input_file(data_dir):
    return copy_file_to_tempdir(small_input_path, data_dir)


@pytest.fixture()
def temp_small_input_file_md5sum():
    return "710781ec9efd25b87bfbf8d6cf4030e9"


def create_large_file(input_file, output_file, target_size):
    # Read the content of the original file
    with open(input_file, encoding='utf8') as infile:
        content = infile.read()
    # Initialize the size of the new file and open it for writing
    current_size = 0
    with open(output_file, 'w') as outfile:
        while current_size < target_size:
            outfile.write(content)
            current_size += len(content)
    return output_file


@pytest.fixture
def temp_large_input_file(data_dir):
    temp_large_input_file_path = data_dir.join('temp_large_input_file.txt')
    target_size = 1024 * 1024 * 6  # create 5MB file, multiupload limit is 5MB
    return create_large_file(small_input_path, temp_large_input_file_path, target_size)


def generate_random_fastq(file_path, size_in_bytes):
    nucleotides = np.array(['A', 'T', 'C', 'G'])
    quality_scores = np.array(list("!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHI"))
    bases_per_read = 100  # Length of each read

    with open(file_path, 'w') as fastq_file:
        total_written = 0

        while total_written < size_in_bytes:
            # Generate a random sequence of nucleotides using numpy
            seq = ''.join(np.random.choice(nucleotides, bases_per_read))
            qual = ''.join(np.random.choice(quality_scores, bases_per_read))

            # FASTQ entry format
            entry = f"@SEQ_ID_{np.random.randint(1, 10 ** 6)}\n{seq}\n+\n{qual}\n"

            fastq_file.write(entry)
            total_written += len(entry)

            # Break if we exceed the size
            if total_written >= size_in_bytes:
                break

        # Adjust the file to match the exact requested size
        fastq_file.truncate(size_in_bytes)


@pytest.fixture
def temp_fastq_gz_file(data_dir):
    file_name = "5M.fastq.gz"
    temp_fastq_gz_path = data_dir.join(file_name)
    target_size = 1024 * 1024 * 6  # create 5MB file, multiupload limit is 5MB

    generate_random_fastq(temp_fastq_gz_path, target_size)

    return temp_fastq_gz_path


@pytest.fixture
def temp_fastq_gz_file_md5sum(temp_fastq_gz_file):
    import hashlib

    with open(temp_fastq_gz_file, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)

    return file_hash.hexdigest()


@pytest.fixture
def temp_crypt4gh_private_key_file(data_dir):
    return copy_file_to_tempdir(private_key_path, data_dir)


@pytest.fixture
def temp_crypt4gh_public_key_file(data_dir):
    return copy_file_to_tempdir(public_key_path, data_dir)


@pytest.fixture
def temp_metadata_file(data_dir, temp_large_input_file):
    with open(metadata_path, "r") as fd:
        metadata = json.load(fd)

    # insert large file
    metadata["Donors"][0]["LabData"][0]["SequenceData"][0]["files"][0]["filepath"] = \
        temp_large_input_file

    metadata_file = data_dir.join("metadata.json")
    with open(metadata_file, 'w') as f:
        f.write(
            metadata.format({"replace_dir": str(data_dir)})
        )
    return str(metadata_file)


config_content = """
public_key_path: '{replace_dir}/mock_public_key.pub'
s3_url: ''
s3_bucket: 'testing'
s3_access_key: 'testing'
s3_secret: 'testing'
"""


@pytest.fixture
def temp_config_file(data_dir):
    config_file = data_dir.join("config.yaml")
    with open(config_file, 'w') as f:
        f.write(config_content.replace("replace_dir", str(data_dir)))
    return str(config_file)


@pytest.fixture
def temp_c4gh_keys(temp_crypt4gh_public_key_file):
    keys = Crypt4GH.prepare_c4gh_keys(temp_crypt4gh_public_key_file)
    return keys


@pytest.fixture
def temp_c4gh_header(temp_c4gh_keys):
    header_pack = Crypt4GH.prepare_header(temp_c4gh_keys)
    return header_pack
