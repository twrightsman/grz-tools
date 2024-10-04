"""
Module: file_operations
This module provides functions for file operations, including calculating the MD5 value of a file,
encrypting files using Crypt4GH, and decrypting files.
Class:
    - Crypt4GH: A class that provides encryption and decryption functionalities using Crypt4GH.
Attributes:
    - Crypt4GH.Key: A type hint for the key used by Crypt4GH.
    - Crypt4GH.VERSION: The version of Crypt4GH.
    - Crypt4GH.SEGMENT_SIZE: The size of each segment for encryption.
    - Crypt4GH.FILE_EXTENSION: The file extension used for encrypted files.
"""
import hashlib
import io
from hashlib import md5, sha256
from os import urandom
from os.path import getsize
import logging
from pathlib import Path
from typing import BinaryIO, Dict, TYPE_CHECKING
from yaml import dump, safe_load, YAMLError


# import crypt4gh
import crypt4gh.header
import crypt4gh.keys
from nacl.bindings import crypto_aead_chacha20poly1305_ietf_encrypt
from nacl.public import PrivateKey
from tqdm.auto import tqdm

# if TYPE_CHECKING:
#     from hashlib import _Hash
# else:
#     _Hash = None

log = logging.getLogger("FileOperations")


def calculate_sha256(file_path, chunk_size=2 ** 16):
    '''
    Calculate the sha256 value of a file in chunks

    :param file_path: pathlib.Path()
    :param chunk_size: int:
    :rtype: string
    :return: calculated sha256 value of file_path
    '''
    total_size = getsize(file_path)
    sha256_hash = sha256()
    with open(file_path, 'rb') as f:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Calculating SHA256 {file_path.name}") as pbar:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                sha256_hash.update(chunk)
                pbar.update(len(chunk))
    return sha256_hash.hexdigest()


def calculate_md5(file_path, chunk_size=2 ** 16):
    """
    Calculate the md5 value of a file in chunks

    :param file_path: pathlib.Path()
    :param chunk_size: int:
    :rtype: string
    :return: calculated md5 value of file_path
    """
    total_size = getsize(file_path)
    md5_hash = md5()
    with open(file_path, "rb") as f:
        with tqdm(
                total=total_size, unit="B", unit_scale=True, desc="Calculating MD5"
        ) as pbar:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                md5_hash.update(chunk)
                pbar.update(len(chunk))
    return md5_hash.hexdigest()

def read_yaml(filepath: Path) -> Dict:
    """
    Method reads in a yaml file.
    
    :param filepath: pathlib.Path()
    :rtype: dict
    :return: The contens of a yaml file as dictionary
    """
    temp_dict = {}
    with open(filepath, "r", encoding="utf-8") as filein:
        try:
            temp_dict = safe_load(filein)
        except YAMLError:
            temp_dict = {}
            for i in format_exc().split("\n"):
                log.error(i)
    return temp_dict

def write_yaml(filepath: Path, content : Dict):
    """
    Method writes a yaml file.
    
    :param filepath: pathlib.Path()
    :param content: dictionary
    """
    log.info(f"Data written to: {filepath}")
    with open(filepath, "w") as fileout:
        dump(content, fileout, default_flow_style=False)

class Crypt4GH(object):
    Key = tuple[int, bytes, bytes]

    VERSION = 1
    SEGMENT_SIZE = 65536
    FILE_EXTENSION = ".c4gh"

    def __init__(self, logger):
        self.__logger = logger

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
    def encrypt_file(input_path, output_path, public_keys):
        """
        Encrypt the file, properly handling the Crypt4GH header.

        :param file_location: pathlib.Path()
        :param s3_object_id: string
        :param keys: tuple[Key]
        :return: tuple with md5 values for original file, encrypted file
        """
        with open(input_path, 'rb') as infile, open(output_path, 'wb') as outfile:

            # prepare header
            header_info = Crypt4GH.prepare_header(public_keys)
            outfile.write(header_info[0])

            while True:
                segment = infile.read(Crypt4GH.SEGMENT_SIZE)  # Read segment directly
                if not segment:
                    # End of file
                    break

                Crypt4GH.encrypt_64k_segment(segment, header_info[1], outfile)

                if len(segment) < Crypt4GH.SEGMENT_SIZE:
                    # End of file
                    break

    @staticmethod
    def decrypt_file(input_path, output_path, private_key):
        raise NotImplementedError()

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
