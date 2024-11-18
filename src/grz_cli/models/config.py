from pathlib import Path
from typing import Annotated

from pydantic import (
    AfterValidator,
    AnyHttpUrl,
    AnyUrl,
    BaseModel,
    ConfigDict,
)
from pydantic.types import PathType


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        use_enum_values=True,
    )


class S3Options(StrictBaseModel):
    endpoint_url: AnyHttpUrl
    """
    The URL for the S3 service.
    """

    bucket: str
    """
    The name of the S3 bucket.
    """

    access_key: str
    """
    The access key for the S3 bucket.
    """

    secret: str
    """
    The secret key for the S3 bucket.
    """

    session_token: str | None = None
    """
    The session token for temporary credentials (optional).
    """

    region_name: str | None = None
    """
    The region name for the S3 bucket.
    """

    api_version: str | None = None
    """
    The S3 API version.
    """

    use_ssl: bool = True
    """
    Whether to use SSL for S3 operations.
    """

    proxy_url: AnyUrl | None = None
    """
    The proxy URL for S3 operations (optional).
    """


FilePath = Annotated[Path, AfterValidator(lambda v: v.expanduser()), PathType("file")]


class ConfigModel(StrictBaseModel):
    grz_public_key_path: FilePath
    """
    Path to the crypt4gh public key of the recipient (the associated GRZ).
    """

    grz_private_key_path: FilePath | None = None
    """
    Path to the crypt4gh private key of the recipient (optional).
    """

    submitter_private_key_path: FilePath | None = None
    """
    Path to the submitter's private key (optional).
    """

    s3_options: S3Options
