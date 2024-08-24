import os  # noqa: D100
from shutil import copyfile

import pytest
from grz_upload.encrypt_upload import prepare_c4gh_keys, prepare_header

config_path = "tests/mock_files/mock_config.yaml"
input_path = "tests/mock_files/mock_input_file.txt"
metadata_path = "tests/mock_files/mock_metadata.csv"
private_key_path = "tests/mock_files/mock_private_key.sec"
public_key_path = "tests/mock_files/mock_public_key.pub"


@pytest.fixture(scope='session')
def datadir(tmpdir_factory):
    """Create temporary folder for the session"""
    datadir = tmpdir_factory.mktemp('data')
    return datadir


def copy_file_to_tempdir(input_path, datadir):
    filename = os.path.basename(input_path)
    target = datadir.join(filename)
    copyfile(input_path, target)
    return target


@pytest.fixture
def temp_log_file(datadir):
    log_file = datadir.join("log.txt")
    return str(log_file)


@pytest.fixture
def temp_input_file(datadir):
    return copy_file_to_tempdir(input_path, datadir)


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
def temp_large_input_file(datadir):
    temp_large_input_file_path = datadir.join('temp_large_input_file.txt')
    target_size = 1024 * 1024 * 6  # create 5MB file, multiupload limit is 5MB
    return create_large_file(input_path, temp_large_input_file_path, target_size)


@pytest.fixture
def temp_private_key_file( datadir):
    return copy_file_to_tempdir(private_key_path, datadir)


@pytest.fixture
def temp_public_key_file(datadir):
    return copy_file_to_tempdir(public_key_path, datadir)


metadata_content = """File id,File Location
test_file,replace_dir/mock_input_file.txt"""


@pytest.fixture
def temp_metadata_file(datadir):
    metadata_file = datadir.join("mock_metadata.csv")
    with open(metadata_file, 'w') as f:
        f.write(metadata_content.replace("replace_dir", str(datadir)))
    return str(metadata_file)


config_content = """metadata_file_path: 'replace_dir/mock_metadata.csv'
public_key_path: 'replace_dir/mock_public_key.pub'
s3_url: ''
s3_bucket: 'testing'
s3_access_key: 'testing'
s3_secret: 'testing'"""


@pytest.fixture
def temp_config_file(datadir):
    config_file = datadir.join("config.yaml")
    with open(config_file, 'w') as f:
        f.write(config_content.replace("replace_dir", str(datadir)))
    return str(config_file)


@pytest.fixture
def temp_c4gh_keys(temp_public_key_file):
    keys = prepare_c4gh_keys(temp_public_key_file)
    return keys


@pytest.fixture
def temp_c4gh_header(temp_c4gh_keys):
    header_pack = prepare_header(temp_c4gh_keys)
    return header_pack

