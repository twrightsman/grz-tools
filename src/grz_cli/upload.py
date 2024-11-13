"""Module for uploading encrypted submissions to a remote storage"""

from __future__ import annotations

import abc
import logging
import math
import re
from os import PathLike, cpu_count
from os.path import getsize
from pathlib import Path
from typing import TYPE_CHECKING, override

import boto3  # type: ignore[import-untyped]
import botocore.handlers  # type: ignore[import-untyped]
from boto3 import client as boto3_client  # type: ignore[import-untyped]
from boto3.s3.transfer import S3Transfer, TransferConfig  # type: ignore[import-untyped]
from botocore.config import Config as Boto3Config  # type: ignore[import-untyped]
from tqdm.auto import tqdm

from .models.config import ConfigModel

MULTIPART_THRESHOLD = 8 * 1024 * 1024  # 8MiB, boto3 default
MULTIPART_CHUNKSIZE = 8 * 1024 * 1024  # 8MiB, boto3 default
MULTIPART_MAX_CHUNKS = 1000  # CEPH S3 limit, AWS limit is 10000

if TYPE_CHECKING:
    from .parser import EncryptedSubmission

log = logging.getLogger(__name__)

# see discussion: https://github.com/boto/boto3/discussions/4251 for acception bucketnames with : in the name
botocore.handlers.VALID_BUCKET = re.compile(r"^[:a-zA-Z0-9.\-_]{1,255}$")  # type: ignore[import-untyped]


class UploadError(Exception):
    """Exception raised when an upload fails"""

    pass


def _gather_files_to_upload(
    encrypted_submission: EncryptedSubmission,
) -> list[tuple[Path, str]]:
    """Gather the files to upload for an encrypted submission"""
    submission_id = encrypted_submission.metadata.index_case_id
    # metadata file is always present
    files = [
        (
            Path(encrypted_submission.metadata.file_path),
            str(Path(submission_id) / "metadata" / "metadata.json"),
        )
    ]

    files += [
        (
            local_file_path,
            str(Path(submission_id) / "files" / file_metadata.encrypted_file_path()),
        )
        for local_file_path, file_metadata in encrypted_submission.encrypted_files.items()
    ]

    return files


class UploadWorker(metaclass=abc.ABCMeta):
    """Worker baseclass for uploading encrypted submissions"""

    def upload(self, encrypted_submission: EncryptedSubmission):
        """
        Upload an encrypted submission

        :param encrypted_submission: The encrypted submission to upload
        :raises UploadError: when the upload failed
        """
        files_to_upload = _gather_files_to_upload(encrypted_submission)

        for local_file_path, _ in files_to_upload:
            if not Path(local_file_path).exists():
                raise UploadError(f"File {local_file_path} does not exist")

        for local_file_path, s3_object_id in files_to_upload:
            try:
                self.upload_file(local_file_path, s3_object_id)
            except Exception as e:
                raise UploadError(
                    f"Failed to upload {local_file_path} (object id: {s3_object_id})"
                ) from e

    @abc.abstractmethod
    def upload_file(self, local_file_path: str | PathLike, s3_object_id: str):
        """
        Upload a single file to the specified object ID
        :param local_file_path: Path to the file to upload
        :param s3_object_id: Remote S3 object ID under which the file should be stored
        """
        raise NotImplementedError()


class S3BotoUploadWorker(UploadWorker):
    """Implementation of an upload worker using boto3 for S3"""

    __log = log.getChild("S3BotoUploadWorker")

    def __init__(
        self,
        config: ConfigModel,
        status_file_path: str | PathLike,
        threads: int | None = None,
    ):
        """
        An upload manager for S3 storage

        :param config: instance of `ConfigModel`
        :param status_file_path: file for storing upload state. Can be used for e.g. resumable uploads.
        """
        super().__init__()

        self._status_file_path = Path(status_file_path)
        self._config = config
        self._threads = threads or cpu_count()

        self._init_s3_client()

    def _init_s3_client(self):
        # if user specifies empty strings, this might be an issue
        def empty_str_to_none(string: str | None) -> str | None:
            if string == "" or string is None:
                return None
            else:
                return string

        # configure proxies if proxy_url is defined
        proxy_url = empty_str_to_none(self._config.s3_options.proxy_url)
        if proxy_url is not None:
            config = Boto3Config(proxies={"http": proxy_url, "https": proxy_url})
        else:
            config = None

        # Initialize S3 client for uploading
        self._s3_client: boto3.session.Session.client = boto3_client(
            service_name="s3",
            region_name=empty_str_to_none(self._config.s3_options.region_name),
            api_version=empty_str_to_none(self._config.s3_options.api_version),
            use_ssl=self._config.s3_options.use_ssl,
            endpoint_url=empty_str_to_none(str(self._config.s3_options.endpoint_url)),
            aws_access_key_id=empty_str_to_none(self._config.s3_options.access_key),
            aws_secret_access_key=empty_str_to_none(self._config.s3_options.secret),
            aws_session_token=empty_str_to_none(self._config.s3_options.session_token),
            config=config,
        )

    def _multipart_upload(self, local_file, s3_object_id):
        """
        Upload the file in chunks to S3.

        :param local_file: pathlib.Path()
        :param s3_object_id: string
        :return: sha256 value for uploaded file
        """
        filesize = getsize(local_file)

        chunksize = (
            math.ceil(filesize / MULTIPART_MAX_CHUNKS)
            if filesize / MULTIPART_CHUNKSIZE > MULTIPART_MAX_CHUNKS
            else MULTIPART_CHUNKSIZE
        )
        self.__log.debug(
            f"Using a chunksize of: {chunksize / 1024**2}MiB, results in {filesize/chunksize} chunks"
        )

        config = TransferConfig(
            multipart_threshold=MULTIPART_THRESHOLD,
            multipart_chunksize=chunksize,
            max_concurrency=self._threads,
        )

        transfer = S3Transfer(self._s3_client, config)
        progress_bar = tqdm(
            total=filesize, unit="B", unit_scale=True, unit_divisor=1024
        )
        transfer.upload_file(
            local_file,
            self._config.s3_options.bucket,
            s3_object_id,
            callback=lambda bytes_transferred: progress_bar.update(bytes_transferred),
        )

    @override
    def upload_file(self, local_file_path, s3_object_id):
        """
        Upload a single file to the specified object ID
        :param local_file_path: Path to the file to upload
        :param s3_object_id: Remote S3 object ID under which the file should be stored
        """
        self.__log.info(f"Uploading {local_file_path} to {s3_object_id}...")

        self._multipart_upload(local_file_path, s3_object_id)
