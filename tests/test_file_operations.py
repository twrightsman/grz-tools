from pathlib import Path
from typing import Tuple

import pytest

from grz_upload.file_operations import (
    calculate_md5,
    calculate_sha256,
    Crypt4GH,
    is_relative_subdirectory,
)


def test_calculate_sha256(temp_small_input_file: str, temp_small_input_file_sha256sum):
    sha256 = calculate_sha256(temp_small_input_file)
    assert isinstance(sha256, str)
    assert len(sha256) == 64  # sha256 hash is 64 characters long
    assert sha256 == temp_small_input_file_sha256sum


def test_calculate_md5(temp_small_input_file: str, temp_small_input_file_md5sum):
    md5 = calculate_md5(temp_small_input_file)
    assert isinstance(md5, str)
    assert len(md5) == 32  # MD5 hash is 32 characters long
    assert md5 == temp_small_input_file_md5sum


def test_prepare_c4gh_keys(temp_crypt4gh_public_key_file: str):
    keys = Crypt4GH.prepare_c4gh_keys(temp_crypt4gh_public_key_file)
    # single key in tupple
    assert len(keys) == 1
    # key method is set to 0
    assert keys[0][0] == 0
    # private key is generated
    assert len(keys[0][1]) == 32


@pytest.mark.parametrize("relative_path, root_directory, expected", [
    # Valid subdirectory paths
    ("root/directory/subdir/file.txt", "root/directory", True),
    ("root/directory/subdir", "root/directory", True),
    ("root/directory/another_subdir/file.txt", "root/directory", True),

    # Target path is exactly the root directory
    ("root/directory", "root/directory", True),

    # Trying to escape root
    ("root/directory/../file_outside.txt", "root/directory", False),
    ("root/directory/../../outside/file.txt", "root/directory", False),

    # Same as root with different formatting
    ("root/directory/.", "root/directory", True),
    ("root/directory/./subdir", "root/directory", True),

    # Completely different path
    ("/some/other/directory/file.txt", "/home/user/projects/root", False),
    ("other/directory", "root/directory", False),
])
def test_is_relative_subdirectory(relative_path, root_directory, expected):
    """
    Test the is_relative_subdirectory() function with various cases.
    """
    result = is_relative_subdirectory(Path(relative_path), Path(root_directory))
    assert result == expected


def test_Crypt4GH_encrypt_file(
        temp_small_input_file: str,
        temp_c4gh_keys: Tuple[Crypt4GH.Key],
        temp_crypt4gh_private_key_file,
        tmp_path_factory
):
    tmp_dir = tmp_path_factory.mktemp("crypt4gh")

    tmp_encrypted_file = tmp_dir / "temp_file.c4gh"
    tmp_decrypted_file = tmp_dir / "temp_file"

    Crypt4GH.encrypt_file(temp_small_input_file, tmp_encrypted_file, temp_c4gh_keys)

    private_key = Crypt4GH.retrieve_private_key(temp_crypt4gh_private_key_file)

    Crypt4GH.decrypt_file(
        tmp_encrypted_file,
        tmp_decrypted_file,
        private_key=[(0, private_key, None)]  # list of (method, privkey, recipient_pubkey=None)
    )

    import filecmp

    assert filecmp.cmp(temp_small_input_file, tmp_decrypted_file)
