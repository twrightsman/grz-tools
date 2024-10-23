from enum import StrEnum

from pydantic import AnyUrl, BaseModel, ConfigDict, model_validator


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        use_enum_values=True,
    )


class Backend(StrEnum):
    """
    The backend to use for S3 operations.
    """

    boto3 = "boto3"
    s3cmd = "s3cmd"


class Boto3(StrictBaseModel):
    pass


class S3cmd(StrictBaseModel):
    """
    Additional configuration for s3cmd (required if backend is 's3cmd').
    """

    host_bucket: str


class S3Options(StrictBaseModel):
    backend: Backend = Backend.boto3
    """
    The backend to use for S3 operations.
    """

    endpoint_url: AnyUrl | str
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

    boto3: Boto3 | None = None
    """
    Additional configuration for boto3.
    """

    s3cmd: S3cmd | None = None
    """
    Additional configuration for s3cmd (required if backend is 's3cmd').
    """

    @model_validator(mode="after")
    def validate_s3_options(self):
        if self.backend == Backend.s3cmd and self.s3cmd is None:
            raise ValueError(
                "s3cmd configuration is required when using the 's3cmd' backend"
            )
        return self


class ConfigModel(StrictBaseModel):
    grz_public_key_path: str
    """
    Path to the crypt4gh public key of the recipient (the associated GRZ).
    """

    grz_private_key_path: str | None = None
    """
    Path to the crypt4gh private key of the recipient (optional).
    """

    submitter_private_key_path: str | None = None
    """
    Path to the submitter's private key (optional).
    """

    s3_options: S3Options
