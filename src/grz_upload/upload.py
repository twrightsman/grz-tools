import io
import logging
from subprocess import Popen, PIPE
from csv import DictWriter
from hashlib import sha256
from os.path import getsize
from traceback import format_exc
from typing import Dict

import boto3
from boto3 import client as boto3_client
from tqdm.auto import tqdm

from grz_upload.file_operations import Crypt4GH

log = logging.getLogger(__name__)


class S3UploadWorker(object):
    CSV_HEADER = [
        "file_id",
        "file_location",
        "original_sha256",
        "filename_encrypted",
        "encrypted_sha256",
        "upload_status",
    ]
    MULTIPART_CHUNK_SIZE = 50 * 1024 * 1024  # 50 MB
    MAX_SINGLEPART_UPLOAD_SIZE = 5 * 1024 * 1024  # 5 MB
    EXT = ".c4gh"

    def __init__(self, s3_dict: Dict[str, str], pubkey_grz_file, status_file_path=None):
        """
        An upload manager for S3 storage

        :param s3_dict: settings for boto3 containing the following fields:
        :param pubkey_grz_file: public key file of the GRZ
        :param status_file_path: optional file for storing upload state. Can be used for e.g. resumable uploads.
        """

        self._status_file_path = status_file_path
        self.__s3_dict = s3_dict
        # self.__pubkey_grz_file = pubkey_grz_file
        # self._keys = Crypt4GH.prepare_c4gh_keys(self.__pubkey_grz_file)
        self.__file_done = 0
        self.__file_failed = 0
        self.__file_total = 0
        self.__file_prefinished = 0

        # Initialize S3 client for uploading
        self.__s3_client: boto3.session.Session.client = boto3_client(
            "s3",
            endpoint_url=(
                self.__s3_dict["s3_url"] if self.__s3_dict["s3_url"] != "" else None
            ),
            aws_access_key_id=self.__s3_dict["s3_access_key"],
            aws_secret_access_key=self.__s3_dict["s3_secret"],
        )



    def show_information(self):
        log.info(f"total files in metafile: {self.__file_total}")
        log.info(f"uploaded files: {self.__file_done}")
        log.info(f"failed files: {self.__file_failed}")
        log.info(
            f"already finished files before current upload: {self.__file_prefinished}"
        )

    def _multipart_upload(self, local_file, s3_object_id):
        """
        Upload the file in chunks to S3.

        :param file_location: pathlib.Path()
        :param s3_object_id: string
        :return: sha256 value for uploaded file
        """
        multipart_upload = self.__s3_client.create_multipart_upload(
            Bucket=self.__s3_dict["s3_bucket"], Key=s3_object_id
        )
        upload_id = multipart_upload["UploadId"]
        parts = []
        part_number = 1

        # Get the file size for progress bar
        file_size = getsize(local_file)
        # initialize progress bar
        progress_bar = tqdm(
            total=file_size, unit="B", unit_scale=True, unit_divisor=1024
        )

        try:
            # Initialize sha256 calculations
            original_sha256 = sha256()

            with open(local_file, "rb") as infile:
                # Process the file in chunks
                while chunk := infile.read(S3UploadWorker.MULTIPART_CHUNK_SIZE):
                    original_sha256.update(chunk)
                    # Upload each chunk
                    part = self.__s3_client.upload_part(
                        Bucket=self.__s3_dict["s3_bucket"],
                        Key=s3_object_id,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk,
                    )
                    progress_bar.update(len(chunk))
                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    part_number += 1

            # Complete the multipart upload
            self.__s3_client.complete_multipart_upload(
                Bucket=self.__s3_dict["s3_bucket"],
                Key=s3_object_id,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            progress_bar.close()  # close progress bar
            return original_sha256.hexdigest()

        except Exception as e:
            for i in format_exc().split("\n"):
                log.error(i)
            self.__s3_client.abort_multipart_upload(
                Bucket=self.__s3_dict["s3_bucket"], Key=s3_object_id, UploadId=upload_id
            )
            raise e

    def _upload(self, local_file, s3_object_id):
        """
        Upload the file to S3.

        :param file_location: pathlib.Path()
        :param s3_object_id: string
        :return: sha256 values for original file
        """
        try:
            with open(local_file, "rb") as fd:
                # calculate sha256sum
                # original_sha256 = sha256(data)

                # Upload data
                self.__s3_client.put_object(
                    Bucket=self.__s3_dict["s3_bucket"], Key=s3_object_id, Body=fd
                )
            # return original_sha256.hexdigest()
        except Exception as e:
            raise e

    def parse_popen_call(self, commlist):
        rvalue = True
        for i in commlist[0].decode('utf-8').split('\n'):
            if i == '': continue
            log.info(i)
        for i in commlist[1].decode('utf-8').split('\n'):
            if i == '': continue
            log.error(i)
            rvalue = False
        return rvalue

    def _upload_s3cmd(self, cfgfile, bucket, local_file):
        commandline = ["s3cmd", "-c", f"{cfgfile}", "--disable-multipart", "put", local_file, bucket]
        print(' '.join(commandline))

        enccall = Popen(commandline, stdout=PIPE, stderr=PIPE)
        enccall.wait()
        self.parse_popen_call(enccall.communicate())

    def _upload_multipart_s3cmd(self, cfgfile, bucket, local_file, chunksize=50):
        commandline = ["s3cmd", "-c", f"{cfgfile}", f"--multipart-chunk-size-mb={chunksize}", "put", local_file, bucket]
        print(' '.join(commandline))

        enccall = Popen(commandline, stdout=PIPE, stderr=PIPE)
        enccall.wait()
        self.parse_popen_call(enccall.communicate())

    def encrypt_upload_files(self, files: Dict[str, str]):
        retval = dict()
        for s3_object_id, local_file_path in files.items():
            log.info(f"{s3_object_id} - {local_file_path} - Processing")

            # Get the file size to decide whether to use multipart upload
            file_size = getsize(local_file_path)
            if file_size > S3UploadWorker.MAX_SINGLEPART_UPLOAD_SIZE:
                # do multipart upload
                sha256sums = self._encrypt_and_multipart_upload(
                    local_file_path, s3_object_id
                )
            else:
                sha256sums = self._encrypt_and_upload(local_file_path, s3_object_id)

            retval[s3_object_id] = sha256sums
        return retval

    def upload_files(self, files: Dict[str, str]):
        retval = dict()
        for s3_object_id, local_file_path in files.items():
            log.info(f"{s3_object_id} - {local_file_path} - Processing")

            # Get the file size to decide whether to use multipart upload
            file_size = getsize(local_file_path)
            if file_size > S3UploadWorker.MAX_SINGLEPART_UPLOAD_SIZE:
                # do multipart upload
                sha256sums = self._multipart_upload(local_file_path, s3_object_id)
            else:
                sha256sums = self._upload(local_file_path, s3_object_id)

            retval[s3_object_id] = sha256sums
        return retval
