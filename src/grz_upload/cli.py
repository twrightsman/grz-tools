"""
CLI module for handling command-line interface operations.
"""

from traceback import format_exc
import logging
import logging.config
import click


from grz_upload.logging_setup import add_filelogger
from grz_upload.parser import Parser

log = logging.getLogger(__name__)


@click.group()
@click.version_option(version="0.1", prog_name="grz_upload")
@click.option("--log-file", metavar="FILE", type=str, help="Path to log file")
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    help="Set the log level (default: INFO)",
)
def cli(log_file=None, log_level="INFO"):
    """
    Command-line interface function for setting up logging.

    Parameters:
    - log_file (str): Path to the log file. If provided, a file logger will be added.
    - log_level (str): Log level for the logger. It should be one of the following:
                       DEBUG, INFO, WARNING, ERROR, CRITICAL.

    Returns:
    None
    """

    if log_file:
        add_filelogger(log_file, log_level)  # Add file logger
    else:
        logging.basicConfig(
            level=log_level.upper(),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    logging.getLogger(__name__).info("Logging setup complete.")

@click.command()
@click.option(
    "-f",
    "--folderpath",
    metavar="STRING",
    type=str,
    required=True,
    help="filepath to the directory containing the data to be uploaded",
)
def checksum_validation(folderpath):
    """
    Validates the checksum of the files in the provided directory.
    Args:
        folderpath (str): The path to the directory containing the data files.
    Raises:
        Exception: If an error occurs during the checksum validation.
        Returns:
        None
    """
    options = {"folderpath": folderpath}

    log.info("starting checksum validation...")
    try:

        parser = Parser(folderpath)
        parser.set_options(options, pubkey=False)
        parser.checksum_validation()
        parser.show_information(logging)

    except (KeyboardInterrupt, Exception) as e:
        log.error(format_exc())

    finally:
        log.info("Shutting Down - Live long and prosper")
        logging.shutdown()


@click.command()
@click.option(
    "-f",
    "--folderpath",
    metavar="STRING",
    type=str,
    required=True,
    help="filepath to the directory containing the data to be uploaded",
)
@click.option(
    "--pubkey_grz",
    metavar="STRING",
    type=str,
    required=True,
    help="public crypt4gh key of the GRZ",
)
def encrypt(folderpath, pubkey_grz):
    """
    Prepares a submission using the provided filepath, metafile, and public key.
    Args:
        folderpath (str): The path to the data files.
        pubkey_grz (str): The public key for the submission.
    Raises:
        Exception: If an error occurs during the preparation of the submission.
    Returns:
        None
    """

    options = {"folderpath": folderpath, "public_key": pubkey_grz}

    log.info("Starting encryption...")

    try:

        parser = Parser(folderpath)
        parser.set_options(options)

        parser.encrypt()

    except (KeyboardInterrupt, Exception) as e:
        log.error(format_exc())

    finally:
        log.info("Shutting Down - Live long and prosper")
        logging.shutdown()


@click.command()
@click.option(
    "-c",
    "--config",
    metavar="STRING",
    type=str,
    required=True,
    help="config file containing the required s3 options",
)
@click.option(
    "-f",
    "--folderpath",
    metavar="STRING",
    type=str,
    required=False,
    help="metafile in json format for data upload to a GRZ s3 structure",
)
# @click.option(
#     "--pubkey_grz",
#     metavar="STRING",
#     type=str,
#     required=True,
#     help="public crypt4gh key of the GRZ",
# )
@click.option(
    "--use_s3cmd",
    metavar="BOOLEAN",
    is_flag = True
    )
def upload(config, folderpath, use_s3cmd):
    """
    Uploads a submission file to s3 using the provided configuration.
    Args:
        config (str): The path to the configuration file.
        pubkey_grz (str): The public key for authentication.
    Returns:
        None
    """

    options = {
        "config_file": config,
        "folderpath" : folderpath,
        "use_s3cmd" : use_s3cmd
    }

    log.info("starting upload")
    try:

        parser = Parser(folderpath)
        parser.set_options(options, False, True)
        parser.upload()

    except (KeyboardInterrupt, Exception) as e:
        log.error(format_exc())

    finally:
        log.info("Shutting Down - Live long and prosper")
        logging.shutdown()


if __name__ == "__main__":
    cli.add_command(checksum_validation)
    cli.add_command(encrypt)
    cli.add_command(upload)
    cli()
