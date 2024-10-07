from __future__ import annotations

import io
import json
import logging
import os
from functools import partial
from getpass import getpass
from hashlib import md5, sha256
from os import urandom
from os.path import getsize
from pathlib import Path
from typing import BinaryIO, TextIO, Tuple

import crypt4gh.header
import crypt4gh.keys
import crypt4gh.lib
from nacl.bindings import crypto_aead_chacha20poly1305_ietf_encrypt
from nacl.public import PrivateKey
from tqdm.auto import tqdm

# if TYPE_CHECKING:
#     from hashlib import _Hash
# else:
#     _Hash = None

log = logging.getLogger(__name__)


def calculate_sha256(file_path: str | Path, chunk_size=2 ** 16, progress=True) -> str:
    '''
    Calculate the sha256 value of a file in chunks

    :param file_path: path to the file
    :param chunk_size: Chunk size in bytes
    :param progress: Print progress
    :return: calculated sha256 value of file_path
    '''
    file_path = Path(file_path)
    total_size = getsize(file_path)
    sha256_hash = sha256()
    with open(file_path, 'rb') as f:
        if progress and (total_size > chunk_size):
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Calculating SHA256 {file_path.name}") as pbar:
                while chunk := f.read(chunk_size):
                    sha256_hash.update(chunk)
                    pbar.update(len(chunk))
        else:
            while chunk := f.read(chunk_size):
                sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def calculate_md5(file_path, chunk_size=2 ** 16, progress=True) -> str:
    """
    Calculate the md5 value of a file in chunks

    :param file_path: path to the file
    :param chunk_size: Chunk size in bytes
    :param progress: Print progress
    :return: calculated md5 value of file_path
    """
    total_size = getsize(file_path)
    md5_hash = md5()
    with open(file_path, "rb") as f:
        if progress and (total_size > chunk_size):
            with tqdm(total=total_size, unit="B", unit_scale=True, desc="Calculating MD5") as pbar:
                while chunk := f.read(chunk_size):
                    md5_hash.update(chunk)
                    pbar.update(len(chunk))
        else:
            while chunk := f.read(chunk_size):
                md5_hash.update(chunk)
    return md5_hash.hexdigest()


def read_multiple_json(input: TextIO, buffer_size=65536, max_buffer_size=134217728):
    decoder = json.JSONDecoder()
    buffer = io.StringIO()  # Use StringIO as the buffer

    while chunk := input.read(buffer_size):
        if len(buffer.getvalue()) + len(chunk) > max_buffer_size:
            raise MemoryError("Reached maximum buffer size while reading input")

        # append chunk to buffer
        buffer.write(chunk)

        while True:
            try:
                data = buffer.getvalue().lstrip()
                # Attempt to decode a JSON object from the current buffer content
                obj, idx = decoder.raw_decode(data)
                yield obj  # Process the decoded object

                # Reset the buffer with the unprocessed content
                buffer = io.StringIO(data[idx:])
            except json.JSONDecodeError as e:
                # If a JSONDecodeError occurs, we need more data, so break out to read the next chunk
                break

    # If there is any remaining content after the loop, try to process it
    remaining_data = buffer.getvalue().strip()
    if remaining_data != "":
        raise ValueError("Remaining data is not empty. Is there invalid JSON?")


def is_relative_subdirectory(relative_path: str | Path, root_directory: str | Path) -> bool:
    """
    Check if the target path is a subdirectory of the root path
    using os.path.commonpath() without checking the file system.

    :param relative_path: The target path.
    :param root_directory: The root directory.
    :return: True if relative_path is a subdirectory of root_directory, otherwise False.
    """
    # Convert both paths to absolute paths without resolving symlinks
    root_directory = os.path.abspath(root_directory)
    relative_path = os.path.abspath(relative_path)

    common_path = os.path.commonpath([root_directory, relative_path])

    # Check if the common path is equal to the root path
    return common_path == root_directory


class Crypt4GH(object):
    Key = tuple[int, bytes, bytes]

    VERSION = 1
    SEGMENT_SIZE = 65536
    FILE_EXTENSION = ".c4gh"

    @staticmethod
    def prepare_c4gh_keys(public_key_file_path: str) -> tuple[Key]:
        """
        Prepare the key format c4gh needs, while it can contain
        multiple keys for multiple recipients, in our use case there is
        a single recipient
        """
        sk = PrivateKey.generate()
        seckey = bytes(sk)
        keys = ((0, seckey, crypt4gh.keys.get_public_key(public_key_file_path)),)
        return keys

    @staticmethod
    def prepare_header(keys: tuple[Key]) -> tuple[bytes, bytes, tuple[Key]]:
        """Prepare header separately to be able to use multiupload"""
        encryption_method = 0  # only choice for this version
        session_key = urandom(32)  # we use one session key for all blocks
        # Output the header
        header_content = crypt4gh.header.make_packet_data_enc(
            encryption_method, session_key
        )
        header_packets = crypt4gh.header.encrypt(header_content, keys)
        header_bytes = crypt4gh.header.serialize(header_packets)
        return (header_bytes, session_key, keys)

    @staticmethod
    def encrypt_64k_segment(data: bytes, session_key: bytes, buffer: BinaryIO):
        """
        Encrypt 64kb block of data with crypt4gh

        :param data: 64kb block of data to encrypt
        :param session_key: The session key with which the data is to be encrypted
        :param buffer: Encrypted data will be written to this buffer
        """
        nonce = urandom(12)
        encrypted_data = crypto_aead_chacha20poly1305_ietf_encrypt(
            data, None, nonce, session_key
        )
        buffer.write(nonce)
        buffer.write(encrypted_data)

    @staticmethod
    def encrypt_part(data: bytes, session_key: bytes, buffer: BinaryIO):
        """
        Encrypt data of arbitrary size with crypt4gh

        :param data: the data to encrypt
        :param session_key: The session key with which the data is to be encrypted
        :param buffer: Encrypted data will be written to this buffer
        """
        data_size = len(data)
        position = 0
        with tqdm(total=data_size, unit="B", unit_scale=True, desc="Encrypting") as pbar:
            while True:
                # Determine how much data to read
                segment_len = min(Crypt4GH.SEGMENT_SIZE, data_size - position)
                if segment_len == 0:  # No more data to read
                    break
                # Read the segment from the byte string
                data_block = data[position: position + segment_len]
                # Update the position
                position += segment_len
                # Process the data in `segment`
                Crypt4GH.encrypt_64k_segment(data_block, session_key, buffer)
                pbar.update(segment_len)

    @staticmethod
    def encrypt_file(input_path, output_path, public_keys: Tuple[Crypt4GH.Key]):
        """
        Encrypt the file, properly handling the Crypt4GH header.

        :param file_location: pathlib.Path()
        :param s3_object_id: string
        :param keys: tuple[Key]
        :return: tuple with md5 values for original file, encrypted file
        """
        # TODO: Progress bar?
        # TODO: store header in separate file?

        with open(input_path, "rb") as in_fd, open(output_path, "wb") as out_fd:
            crypt4gh.lib.encrypt(
                keys=public_keys,
                infile=in_fd,
                outfile=out_fd,
            )

        # with open(input_path, 'rb') as infile, open(output_path, 'wb') as outfile:
        #
        #     # prepare header
        #     header_info = Crypt4GH.prepare_header(public_keys)
        #     outfile.write(header_info[0])
        #
        #     while True:
        #         segment = infile.read(Crypt4GH.SEGMENT_SIZE)  # Read segment directly
        #         if not segment:
        #             # End of file
        #             break
        #
        #         Crypt4GH.encrypt_64k_segment(segment, header_info[1], outfile)
        #
        #         if len(segment) < Crypt4GH.SEGMENT_SIZE:
        #             # End of file
        #             break

    @staticmethod
    def retrieve_private_key(seckey_path):
        seckeypath = os.path.expanduser(seckey_path)
        if not os.path.exists(seckeypath):
            raise ValueError('Secret key not found')

        passphrase = os.getenv('C4GH_PASSPHRASE')
        if passphrase:
            passphrase_callback = lambda: passphrase
        else:
            passphrase_callback = partial(getpass, prompt=f'Passphrase for {seckey_path}: ')

        return crypt4gh.keys.get_private_key(seckeypath, passphrase_callback)

    @staticmethod
    def decrypt_file(input_path, output_path, private_key):
        with open(input_path, "rb") as in_fd, open(output_path, "wb") as out_fd:
            crypt4gh.lib.decrypt(
                keys=private_key,
                infile=in_fd,
                outfile=out_fd,
            )

# class HashLoggingWriter(io.RawIOBase):
#     def __init__(self, binary_io: io.RawIOBase, hash_algorithm: str = 'sha256'):
#         self._binary_io = binary_io  # The underlying binary I/O object
#         self.hash_algorithm = hash_algorithm  # Hashing algorithm (e.g., 'md5', 'sha256', etc.)
#
#         # Create the hash object based on the specified algorithm
#         if hash_algorithm not in hashlib.algorithms_available:
#             raise ValueError(f"Unsupported hash algorithm: {hash_algorithm}")
#         self.hash_obj = hashlib.new(hash_algorithm)
#
#     def write(self, b: bytes) -> int:
#         # Update the hash with the bytes being written
#         self.hash_obj.update(b)
#         # Write the bytes to the underlying binary IO object
#         return self._binary_io.write(b)
#
#     def read(self, size=-1) -> bytes:
#         raise NotImplementedError("This class is write-only")
#
#     def readinto(self, b: bytearray) -> int:
#         raise NotImplementedError("This class is write-only")
#
#     def flush(self):
#         # Ensure all data is flushed to the underlying binary IO object
#         self._binary_io.flush()
#
#     def close(self):
#         # Close the underlying binary IO object
#         self._binary_io.close()
#
#     def get_hash(self) -> _Hash:
#         # Return the hex digest of the hash
#         return self.hash_obj
