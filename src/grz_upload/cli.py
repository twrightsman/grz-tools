"""
CLI module for handling command-line interface operations.
"""

import logging
import logging.config
from os import PathLike
from pathlib import Path
from traceback import format_exc

import click
import yaml

from grz_upload.constants import PACKAGE_ROOT
from grz_upload.logging_setup import add_filelogger
from grz_upload.parser import Worker

# replace __MAIN__ with correct module name
log = logging.getLogger(PACKAGE_ROOT + ".cli")


class OrderedGroup(click.Group):
    def list_commands(self, ctx):
        # Return commands in the order they were added
        return list(self.commands.keys())


@click.group(cls=OrderedGroup)
@click.version_option(version="0.1", prog_name="grz-cli")
@click.option("--log-file", metavar="FILE", type=str, help="Path to log file")
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    help="Set the log level (default: INFO)",
)
def cli(log_file: str | None = None, log_level: str = "INFO"):
    """
    Command-line interface function for setting up logging.

    :param log_file: Path to the log file. If provided, a file logger will be added.
    :param log_level: Log level for the logger. It should be one of the following:
                       DEBUG, INFO, WARNING, ERROR, CRITICAL.
    """
    if log_file:
        add_filelogger(
            log_file,
            log_level.upper(),
        )  # Add file logger

    # show only time and log level in STDOUT
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # set the log level for this package
    logging.getLogger(PACKAGE_ROOT).setLevel(log_level.upper())

    log.debug("Logging setup complete.")


@cli.command()
@click.option(
    "-s",
    "--submission-dir",
    metavar="STRING",
    type=click.Path(
        exists=True, file_okay=False, dir_okay=True, readable=True, resolve_path=True
    ),
    required=True,
    help="Path to the submission directory containing both 'metadata/' and 'files/' directories",
)
@click.option(
    "-m",
    "--metadata-dir",
    metavar="STRING",
    type=click.Path(
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    help="Path to the metadata directory containing the metadata.json file",
)
@click.option(
    "-f",
    "--files-dir",
    metavar="STRING",
    type=click.Path(
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    help="Path to the files linked in the submission",
)
@click.option(
    "-w",
    "--working-dir",
    metavar="STRING",
    type=click.Path(
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    callback=lambda c, p, v: v if v else c.params["submission_dir"],
    help="Path to a working directory where intermediate files can be stored",
)
def validate(submission_dir: str, metadata_dir: str, files_dir: str, working_dir: str):
    """
    Validates the sha256 checksum of the sequence data files. This command must be executed before the encryption and upload can start.
    """
    log.info("Starting validation...")

    working_dir = Path(working_dir)
    submission_dir = Path(submission_dir)

    worker_inst = Worker(
        working_dir=working_dir,
        files_dir=submission_dir / "files" if files_dir is None else files_dir,
        metadata_dir=(
            submission_dir / "metadata" if metadata_dir is None else metadata_dir
        ),
    )
    worker_inst.validate()

    log.info("Validation finished!")


@cli.command()
@click.option(
    "-s",
    "--submission-dir",
    metavar="STRING",
    type=click.Path(
        exists=True, file_okay=False, dir_okay=True, readable=True, resolve_path=True
    ),
    required=True,
    help="Path to the submission directory containing both 'metadata/' and 'files/' directories",
)
@click.option(
    "-m",
    "--metadata-dir",
    metavar="STRING",
    type=click.Path(
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    help="Path to the metadata directory containing the metadata.json file",
)
@click.option(
    "-f",
    "--files-dir",
    metavar="STRING",
    type=click.Path(
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    help="Path to the files linked in the submission",
)
@click.option(
    "-w",
    "--working-dir",
    metavar="STRING",
    type=click.Path(
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    callback=lambda c, p, v: v if v else c.params["submission_dir"],
    help="Path to a working directory where intermediate files can be stored",
)
@click.option(
    "-o",
    "--encrypted-files-dir",
    metavar="STRING",
    type=click.Path(
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    help="Path to a directory where the encrypted files can be stored",
)
@click.option(
    "-c",
    "--config-file",
    metavar="STRING",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True
    ),
    required=True,
    default="~/.config/grz_upload/config.yaml",
    help="Path to config file",
)
def encrypt(  # noqa: PLR0913
    submission_dir,
    working_dir,
    metadata_dir,
    files_dir,
    encrypted_files_dir,
    config_file,
):
    """
    Encrypt a submission using the GRZ public key.
    """
    with open(config_file) as f:
        config = yaml.safe_load(f)

    grz_pubkey_path = config["grz_public_key_path"]

    submitter_privkey_path = config.get("submitter_private_key_path", None)
    if submitter_privkey_path == "":
        submitter_privkey_path = None

    log.info("Starting encryption...")

    working_dir = Path(working_dir)
    submission_dir = Path(submission_dir)

    worker_inst = Worker(
        working_dir=working_dir,
        encrypted_files_dir=encrypted_files_dir,
        files_dir=submission_dir / "files" if files_dir is None else files_dir,
        metadata_dir=(
            submission_dir / "metadata" if metadata_dir is None else metadata_dir
        ),
    )
    worker_inst.encrypt(
        grz_pubkey_path, submitter_private_key_path=submitter_privkey_path
    )

    log.info("Encryption successful!")


@cli.command()
@click.option(
    "-s",
    "--encrypted-submission-dir",
    metavar="STRING",
    type=click.Path(
        exists=True, file_okay=False, dir_okay=True, readable=True, resolve_path=True
    ),
    required=True,
    help="Path to the encrypted submission directory containing both 'metadata/' and 'encrypted_files/' directories",
)
@click.option(
    "-m",
    "--metadata-dir",
    metavar="STRING",
    type=click.Path(
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    help="Path to the metadata directory containing the metadata.json file",
)
@click.option(
    "-e",
    "--encrypted-files-dir",
    metavar="STRING",
    type=click.Path(
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    help="Path to the encrypted files linked in the submission",
)
@click.option(
    "-w",
    "--working-dir",
    metavar="STRING",
    type=click.Path(
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    callback=lambda c, p, v: v if v else c.params["submission_dir"],
    help="Path to a working directory where intermediate files can be stored",
)
@click.option(
    "-o",
    "--decrypted-files-dir",
    metavar="STRING",
    type=click.Path(
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    help="Path to a directory where the decrypted files can be stored",
)
@click.option(
    "-c",
    "--config-file",
    metavar="STRING",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True
    ),
    required=True,
    default="~/.config/grz_upload/config.yaml",
    help="Path to config file",
)
def decrypt(  # noqa: PLR0913
    encrypted_submission_dir: str,
    metadata_dir: str,
    encrypted_files_dir: str,
    working_dir: str,
    decrypted_files_dir: str,
    config_file: str,
):
    """
    Decrypt a submission using the GRZ private key.
    """
    with open(config_file) as f:
        config = yaml.safe_load(f)

    grz_privkey_path = config["grz_private_key_path"]

    log.info("Starting encryption...")

    working_dir = Path(working_dir)
    encrypted_submission_dir = Path(encrypted_submission_dir)

    worker_inst = Worker(
        working_dir=working_dir,
        files_dir=working_dir / "files"
        if decrypted_files_dir is None
        else decrypted_files_dir,
        metadata_dir=(
            encrypted_submission_dir / "metadata"
            if metadata_dir is None
            else metadata_dir
        ),
        encrypted_files_dir=(
            encrypted_submission_dir / "encrypted_files"
            if encrypted_files_dir is None
            else encrypted_files_dir
        ),
    )
    worker_inst.decrypt(grz_privkey_path)

    log.info("Encryption successful!")


@cli.command()
@click.option(
    "-s",
    "--encrypted-submission-dir",
    metavar="STRING",
    type=click.Path(
        exists=True, file_okay=False, dir_okay=True, readable=True, resolve_path=True
    ),
    required=True,
    help="Path to the encrypted submission directory containing both 'metadata/' and 'encrypted_files/' directories",
)
@click.option(
    "-m",
    "--metadata-dir",
    metavar="STRING",
    type=click.Path(
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    help="Path to the metadata directory containing the metadata.json file",
)
@click.option(
    "-e",
    "--encrypted-files-dir",
    metavar="STRING",
    type=click.Path(
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    help="Path to the encrypted files linked in the submission",
)
@click.option(
    "-w",
    "--working-dir",
    metavar="STRING",
    type=click.Path(
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    required=False,
    default=None,
    callback=lambda c, p, v: v if v else c.params["folderpath"],
    help="Path to a working directory where intermediate files can be stored",
)
@click.option(
    "-c",
    "--config-file",
    metavar="STRING",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True
    ),
    required=True,
    default="~/.config/grz_upload/config.yaml",
    help="Path to config file",
)
def upload(
    encrypted_submission_dir,
    metadata_dir,
    encrypted_files_dir,
    working_dir,
    config_file,
):
    """
    Uploads a submission file to s3 using the provided configuration.
    :param config: The path to the configuration file.
    :param folderpath: Path to a working directory where intermediate files can be stored
    :param use_s3cmd: Whether to use s3cmd for the upload or builtin boto3
    """
    with open(config_file) as f:
        config = yaml.safe_load(f)

    log.info("Starting upload...")
    working_dir = Path(working_dir)
    encrypted_submission_dir = Path(encrypted_submission_dir)

    worker_inst = Worker(
        working_dir=working_dir,
        metadata_dir=(
            encrypted_submission_dir / "metadata"
            if metadata_dir is None
            else metadata_dir
        ),
        encrypted_files_dir=(
            encrypted_submission_dir / "encrypted_files"
            if encrypted_files_dir is None
            else encrypted_files_dir
        ),
    )
    worker_inst.upload(config)

    log.info("Upload finished!")


if __name__ == "__main__":
    cli()
