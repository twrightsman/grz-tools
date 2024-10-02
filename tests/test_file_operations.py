import io
from math import ceil

from grz_upload.file_operations import (
    calculate_md5,
    calculate_sha256,
    Crypt4GH,
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


def test_Crypt4GH_prepare_header(temp_c4gh_keys: tuple[Crypt4GH.Key]):
    header_pack = Crypt4GH.prepare_header(temp_c4gh_keys)
    assert len(header_pack) == 3
    # assert header size
    assert len(header_pack[0]) == 124
    # size of session key
    assert len(header_pack[1]) == 32
    # pass back the c4gh keys
    assert header_pack[2] == temp_c4gh_keys


def test_Crypt4GH_encrypt_segment(temp_c4gh_header: tuple[bytes, bytes, tuple[Crypt4GH.Key]]):
    data = b"test segment content"
    key = temp_c4gh_header[1]
    buffer = io.BytesIO()

    Crypt4GH.encrypt_64k_segment(data, key, buffer=buffer)
    encrypted_data = buffer.getvalue()

    assert isinstance(encrypted_data, bytes)
    # size is larger by 12 nonce and 16 mac(?)
    assert len(encrypted_data) == len(data) + 12 + 16


def test_Crypt4GH_encrypt_part(temp_small_input_file: str, temp_c4gh_header: tuple[bytes, bytes, tuple[Crypt4GH.Key]]):
    with open(temp_small_input_file, 'rb') as infile:
        byte_string = infile.read()
        session_key = temp_c4gh_header[1]
        buffer = io.BytesIO()

        Crypt4GH.encrypt_part(byte_string, session_key, buffer=buffer)
        encrypted_part = buffer.getvalue()

    file_size = len(byte_string)
    segment_no = ceil(file_size / Crypt4GH.SEGMENT_SIZE)
    expected_encrypted_part_size = file_size + segment_no * (12 + 16)

    assert expected_encrypted_part_size == len(encrypted_part)


def test_Crypt4GH_encrypt_file(
        temp_small_input_file: str,
        temp_c4gh_keys: tuple[Crypt4GH.Key],
        temp_crypt4gh_private_key_file,
        tmp_path_factory
):
    tmp_dir = tmp_path_factory.mktemp("crypt4gh")

    tmp_encrypted_file = tmp_dir / "temp_file.c4gh"
    tmp_decrypted_file = tmp_dir / "temp_file"

    Crypt4GH.encrypt_file(temp_small_input_file, tmp_encrypted_file, temp_c4gh_keys)
    Crypt4GH.decrypt_file(tmp_encrypted_file, tmp_decrypted_file, temp_crypt4gh_private_key_file)

    import filecmp

    assert filecmp.cmp(tmp_encrypted_file, tmp_decrypted_file)
