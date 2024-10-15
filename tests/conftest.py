from __future__ import annotations

import json
import os  # noqa: D100
from pathlib import Path
from shutil import copyfile

import numpy as np
import pytest
import yaml

from grz_upload.file_operations import Crypt4GH

config_path = "tests/mock_files/mock_config.yaml"
small_file_input_path = "tests/mock_files/mock_small_input_file.txt"
metadata_path = "tests/mock_files/example_metadata.json"

crypt4gh_private_key_file = "tests/mock_files/mock_private_key.sec"
crypt4gh_public_key_file = "tests/mock_files/mock_public_key.pub"


@pytest.fixture()
def crypt4gh_private_key_file_path():
    return Path(crypt4gh_private_key_file)


@pytest.fixture()
def crypt4gh_public_key_file_path():
    return Path(crypt4gh_public_key_file)


@pytest.fixture(scope="session")
def temp_data_dir(tmpdir_factory: pytest.TempdirFactory):
    """Create temporary folder for the session"""
    datadir = tmpdir_factory.mktemp("data")
    return datadir


@pytest.fixture
def temp_data_dir_path(temp_data_dir) -> Path:
    return Path(temp_data_dir.strpath)


def copy_file_to_tempdir(input_path: str | Path, datadir: str | Path) -> Path:
    filename = os.path.basename(input_path)
    target = Path(datadir) / filename

    copyfile(input_path, target)

    return target


@pytest.fixture
def temp_small_file_path(temp_data_dir_path) -> Path:
    return copy_file_to_tempdir(small_file_input_path, temp_data_dir_path)


@pytest.fixture()
def temp_small_file_md5sum():
    return "710781ec9efd25b87bfbf8d6cf4030e9"


@pytest.fixture()
def temp_small_file_sha256sum():
    return "78858035d88f0c66d27984789ddd8fa8a8fc633cf7689ac2b4b1e2e7b37ee3be"


def create_large_file(
    content: str | bytes, output_file: str | Path, target_size: int
) -> int:
    """
    Write some content repeatedly to a file until some target size is reached.

    :param content: Content of the file. Will be repeatedly written to output_file until `target_size` is reached.
    :param output_file: Path to the output file.
    :param target_size: target size in bytes.
    :return: Actual bytes written
    """
    # Initialize the size of the new file and open it for writing
    current_size = 0
    with open(output_file, "w") as outfile:
        while current_size < target_size:
            bytes_written = outfile.write(content)
            current_size += bytes_written
    return current_size


@pytest.fixture
def temp_large_file_path(temp_data_dir_path) -> Path:
    temp_large_file_path = temp_data_dir_path / "temp_large_input_file.txt"
    target_size = 1024 * 1024 * 6  # create 5MB file, multiupload limit is 5MB

    with open(small_file_input_path) as fd:
        content = fd.read()

    create_large_file(content, temp_large_file_path, target_size)

    return temp_large_file_path


def generate_random_fastq(file_path: str | Path, target_size: int) -> int:
    """
    Create a random FASTQ file.

    :param file_path: Path to the FASTQ file.
    :param target_size: Target size in bytes.
    :return: Actual bytes written
    """
    nucleotides = np.array(["A", "T", "C", "G"])
    quality_scores = np.array(list("!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHI"))
    bases_per_read = 100  # Length of each read

    with open(file_path, "w") as fastq_file:
        total_bytes_written = 0

        while total_bytes_written < target_size:
            # Generate a random sequence of nucleotides using numpy
            seq = "".join(np.random.choice(nucleotides, bases_per_read))
            qual = "".join(np.random.choice(quality_scores, bases_per_read))

            # FASTQ entry format
            entry = f"@SEQ_ID_{np.random.randint(1, 10 ** 6)}\n{seq}\n+\n{qual}\n"

            actual_bytes_written = fastq_file.write(entry)
            total_bytes_written += actual_bytes_written

    return total_bytes_written


@pytest.fixture
def temp_fastq_file_path(temp_data_dir_path) -> Path:
    file_name = "5M.fastq"
    temp_fastq_gz_path = temp_data_dir_path / file_name
    target_size = 1024 * 1024 * 6  # create 5MB file, multiupload limit is 5MB

    generate_random_fastq(temp_fastq_gz_path, target_size)

    return temp_fastq_gz_path


@pytest.fixture
def temp_fastq_file_md5sum(temp_fastq_file_path):
    import hashlib

    with open(temp_fastq_file_path, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)

    return file_hash.hexdigest()


@pytest.fixture
def temp_fastq_file_sha256sum(temp_fastq_file_path):
    import hashlib

    with open(temp_fastq_file_path, "rb") as f:
        file_hash = hashlib.sha256()
        while chunk := f.read(8192):
            file_hash.update(chunk)

    return file_hash.hexdigest()


# @pytest.fixture
# def crypt4gh_private_key_file(temp_data_dir_path):
#     return copy_file_to_tempdir(private_key_path, temp_data_dir_path)
#
#
# @pytest.fixture
# def crypt4gh_public_key_file(temp_data_dir_path):
#     return copy_file_to_tempdir(public_key_path, temp_data_dir_path)


@pytest.fixture
def temp_metadata_file_path(temp_data_dir_path, temp_large_file_path) -> Path:
    with open(metadata_path) as fd:
        metadata = json.load(fd)

    # insert large file
    metadata["Donors"][0]["labData"][0]["sequenceData"][0]["files"][0]["filepath"] = (
        str(temp_large_file_path)
    )

    metadata_file_path = temp_data_dir_path / "metadata.json"
    with open(metadata_file_path, "w") as fd:
        json.dump(metadata, fd)

    return metadata_file_path


@pytest.fixture
def config_content(crypt4gh_public_key_file_path):
    return {
        "public_key_path": str(crypt4gh_public_key_file_path),
        "s3_url": "",
        "s3_bucket": "testing",
        "s3_access_key": "testing",
        "s3_secret": "testing",
    }


@pytest.fixture
def temp_config_file_path(config_content, temp_data_dir_path) -> Path:
    config_file = temp_data_dir_path / "config.yaml"
    with open(config_file, "w") as fd:
        yaml.dump(config_content, fd)
    return config_file


@pytest.fixture
def c4gh_public_keys(crypt4gh_public_key_file_path):
    keys = Crypt4GH.prepare_c4gh_keys(crypt4gh_public_key_file_path)
    return keys
