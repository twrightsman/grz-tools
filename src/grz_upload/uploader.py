import logging
from csv import DictWriter
from hashlib import md5
from os.path import getsize
from traceback import format_exc
from typing import Dict

import boto3
from boto3 import client as boto3_client
from tqdm.auto import tqdm

from grz_upload.file_operations import Crypt4GH

log = logging.getLogger(__name__)


class S3UploadWorker(object):
    CSV_HEADER = ['file_id', 'file_location', 'original_md5', 'filename_encrypted', 'encrypted_md5', 'upload_status']
    MULTIPART_CHUNK_SIZE = 50 * 1024 * 1024  # 50 MB
    MAX_SINGLEPART_UPLOAD_SIZE = 5 * 1024 * 1024  # 5 MB
    EXT = '.c4gh'

    def __init__(self, s3_dict: Dict[str, str], pubkey_grz_file, status_file_path=None):
        """
        An upload manager for S3 storage

        :param s3_dict: settings for boto3 containing the following fields:
        :param pubkey_grz_file: public key file of the GRZ
        :param status_file_path: optional file for storing upload state. Can be used for e.g. resumable uploads.
        """

        self._status_file_path = status_file_path
        self.__s3_dict = s3_dict
        self.__pubkey_grz_file = pubkey_grz_file
        self._keys = Crypt4GH.prepare_c4gh_keys(self.__pubkey_grz_file)
        self.__file_done = 0
        self.__file_failed = 0
        self.__file_total = 0
        self.__file_prefinished = 0

        # Initialize S3 client for uploading
        self.__s3_client: boto3.session.Session.client = boto3_client(
            's3',
            endpoint_url=self.__s3_dict['s3_url'] if self.__s3_dict['s3_url'] != "" else None,
            aws_access_key_id=self.__s3_dict['s3_access_key'],
            aws_secret_access_key=self.__s3_dict['s3_secret']
        )

    def save_upload_state(self):
        '''
       Save the upload state as a csv file to the filesystem
       '''
        log.info(f'Upload status information written to: {self._status_file_path}')
        with open(self._status_file_path, 'w', newline='') as temp_csvfile:
            writer = DictWriter(temp_csvfile, fieldnames=S3UploadWorker.CSV_HEADER, delimiter=',')
            writer.writeheader()
            for file_dict in self._status_file_path.values():
                writer.writerow(file_dict)

    def show_information(self):
        log.info(f'total files in metafile: {self.__file_total}')
        log.info(f'uploaded files: {self.__file_done}')
        log.info(f'failed files: {self.__file_failed}')
        log.info(f'already finished files before current upload: {self.__file_prefinished}')

    def _encrypt_and_multipart_upload(self, local_file, s3_object_id):
        """
        Encrypt and upload the file in chunks, properly handling the Crypt4GH header.

        :param file_location: pathlib.Path()
        :param s3_object_id: string
        :return: tuple with md5 values for original file, encrypted file
        """
        multipart_upload = self.__s3_client.create_multipart_upload(
            Bucket=self.__s3_dict['s3_bucket'],
            Key=s3_object_id
        )
        upload_id = multipart_upload['UploadId']
        parts = []
        part_number = 1

        # Get the file size for progress bar
        file_size = getsize(local_file)
        # initialize progress bar
        progress_bar = tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024)

        try:
            # Initialize MD5 calculations
            original_md5 = md5()
            encrypted_md5 = md5()

            # prepare header
            header_info = Crypt4GH.prepare_header(self._keys)

            with open(local_file, 'rb') as infile:
                # we will prepend the header to the first chunk
                first_chunk_read = False

                # Process and encrypt the file in chunks
                while chunk := infile.read(S3UploadWorker.MULTIPART_CHUNK_SIZE):
                    original_md5.update(chunk)

                    encrypted_chunk = Crypt4GH.encrypt_part(chunk, header_info[1])

                    # add header to the first chunk
                    if not first_chunk_read:
                        encrypted_chunk = header_info[0] + encrypted_chunk
                        first_chunk_read = True

                    encrypted_md5.update(encrypted_chunk)

                    # Upload each encrypted chunk
                    part = self.__s3_client.upload_part(
                        Bucket=self.__s3_dict['s3_bucket'],
                        Key=s3_object_id,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=encrypted_chunk
                    )
                    progress_bar.update(len(chunk))

                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    part_number += 1

            # Complete the multipart upload
            self.__s3_client.complete_multipart_upload(
                Bucket=self.__s3_dict['s3_bucket'],
                Key=s3_object_id,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            progress_bar.close()  # close progress bar
            return original_md5.hexdigest(), encrypted_md5.hexdigest()

        except Exception as e:
            for i in format_exc().split('\n'): log.error(i)
            self.__s3_client.abort_multipart_upload(
                Bucket=self.__s3_dict['s3_bucket'],
                Key=s3_object_id,
                UploadId=upload_id
            )
            raise e

    def _encrypt_and_upload(self, local_file, s3_object_id):
        """
        Encrypt and upload the file, properly handling the Crypt4GH header.

        :param file_location: pathlib.Path()
        :param s3_object_id: string
        :param keys: tuple[Key]
        :return: tuple with md5 values for original file, encrypted file
        """
        try:
            # prepare header
            header_info = Crypt4GH.prepare_header(self._keys)

            # read the whole file into memory
            with open(local_file, 'rb') as fd:
                data = fd.read()

            encrypted_data = Crypt4GH.encrypt_part(data, header_info[1])
            # add header
            encrypted_data = header_info[0] + encrypted_data

            # Calculate MD5 sums
            original_md5 = md5(data)
            encrypted_md5 = md5(encrypted_data)

            # Upload data
            self.__s3_client.put_object(
                Bucket=self.__s3_dict['s3_bucket'],
                Key=s3_object_id,
                Body=encrypted_data
            )

            return original_md5.hexdigest(), encrypted_md5.hexdigest()

        except Exception as e:
            raise e

    def _multipart_upload(self, local_file, s3_object_id):
        """
        Upload the file in chunks to S3.

        :param file_location: pathlib.Path()
        :param s3_object_id: string
        :return: md5 value for uploaded file
        """
        multipart_upload = self.__s3_client.create_multipart_upload(
            Bucket=self.__s3_dict['s3_bucket'],
            Key=s3_object_id
        )
        upload_id = multipart_upload['UploadId']
        parts = []
        part_number = 1

        # Get the file size for progress bar
        file_size = getsize(local_file)
        # initialize progress bar
        progress_bar = tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024)

        try:
            # Initialize MD5 calculations
            original_md5 = md5()

            with open(local_file, 'rb') as infile:
                # Process the file in chunks
                while chunk := infile.read(S3UploadWorker.MULTIPART_CHUNK_SIZE):
                    original_md5.update(chunk)
                    # Upload each chunk
                    part = self.__s3_client.upload_part(
                        Bucket=self.__s3_dict['s3_bucket'],
                        Key=s3_object_id,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )
                    progress_bar.update(len(chunk))
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    part_number += 1

            # Complete the multipart upload
            self.__s3_client.complete_multipart_upload(
                Bucket=self.__s3_dict['s3_bucket'],
                Key=s3_object_id,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            progress_bar.close()  # close progress bar
            return original_md5.hexdigest()

        except Exception as e:
            for i in format_exc().split('\n'): log.error(i)
            self.__s3_client.abort_multipart_upload(
                Bucket=self.__s3_dict['s3_bucket'],
                Key=s3_object_id,
                UploadId=upload_id
            )
            raise e

    def _upload(self, local_file, s3_object_id):
        """
        Upload the file to S3.

        :param file_location: pathlib.Path()
        :param s3_object_id: string
        :return: md5 values for original file
        """
        try:
            # read the whole file into memory
            with open(local_file, 'rb') as fd:
                data = fd.read()

            # calculate md5sum
            original_md5 = md5(data)

            # Upload data
            self.__s3_client.put_object(
                Bucket=self.__s3_dict['s3_bucket'],
                Key=s3_object_id,
                Body=data
            )

            return original_md5.hexdigest()

        except Exception as e:
            raise e

    def encrypt_upload_files(self, files: Dict[str, str]):
        retval = dict()
        for s3_object_id, local_file_path in files.items():
            log.info(f'{s3_object_id} - {local_file_path} - Processing')

            # Get the file size to decide whether to use multipart upload
            file_size = getsize(local_file_path)
            if file_size > S3UploadWorker.MAX_SINGLEPART_UPLOAD_SIZE:
                # do multipart upload
                md5sums = self._encrypt_and_multipart_upload(local_file_path, s3_object_id)
            else:
                md5sums = self._encrypt_and_upload(local_file_path, s3_object_id)

            retval[s3_object_id] = md5sums
        return retval

    def upload_files(self, files: Dict[str, str]):
        retval = dict()
        for s3_object_id, local_file_path in files.items():
            log.info(f'{s3_object_id} - {local_file_path} - Processing')

            # Get the file size to decide whether to use multipart upload
            file_size = getsize(local_file_path)
            if file_size > S3UploadWorker.MAX_SINGLEPART_UPLOAD_SIZE:
                # do multipart upload
                md5sums = self._multipart_upload(local_file_path, s3_object_id)
            else:
                md5sums = self._upload(local_file_path, s3_object_id)

            retval[s3_object_id] = md5sums
        return retval
