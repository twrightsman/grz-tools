import os  # noqa: D100
from shutil import copyfile

import pytest

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


def copy_file_to_tempdir(input_path, datadir):  # noqa: F821
    print(datadir)
    filename = os.path.basename(input_path)
    target = datadir.join(filename)
    copyfile(input_path, target)
    return target


@pytest.fixture
def temp_log_file(datadir):
    print(datadir)
    log_file = datadir.join("log.txt")
    return str(log_file)


@pytest.fixture
def temp_input_file(datadir):
    print(datadir)
    return copy_file_to_tempdir(input_path, datadir)


@pytest.fixture
def temp_private_key_file( datadir):
    print(datadir)
    return copy_file_to_tempdir(private_key_path, datadir)


@pytest.fixture
def temp_public_key_file(datadir):
    return copy_file_to_tempdir(public_key_path, datadir)


metadata_content = """File id,File Location
test_file,replace_dir/mock_input_file.txt"""


@pytest.fixture
def temp_metadata_file(datadir):
    print(datadir)
    metadata_file = datadir.join("mock_metadata.csv")
    print(metadata_file)
    with open(metadata_file, 'w') as f:
        f.write(metadata_content.replace("replace_dir", str(datadir)))
    return str(metadata_file)


config_content = """metadata_file_path: 'replace_dir/mock_metadata.csv'
public_key_path: 'replace_dir/mock_public_key.pub'
s3_url: 'testing'
s3_bucket: 'testing'
s3_access_key: 'testing'
s3_secret: 'testing'"""


@pytest.fixture
def temp_config_file(datadir):
    print(datadir)
    config_file = datadir.join("config.yaml")
    print(config_file)
    with open(config_file, 'w') as f:
        f.write(config_content.replace("replace_dir", str(datadir)))
    return str(config_file)