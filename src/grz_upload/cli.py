"""
CLI module for handling command-line interface operations.
"""

''' python modules '''
import logging
import logging.config
import click
from pathlib import Path
from traceback import format_exc

''' package modules '''
from grz_upload.file_operations import write_yaml
from grz_upload.logging_setup import add_filelogger
from grz_upload.parser import Worker

log = logging.getLogger('ArgumentParser')


@click.group()
@click.version_option(version="0.1", prog_name="grz_upload")
@click.option("--log-file", metavar="FILE", type=str, help="Path to log file")
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    help="Set the log level (default: INFO)",
)
def cli(log_file: str = None, log_level: str = "INFO"):
    """
    Command-line interface function for setting up logging.

    :param log_file:
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
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True, resolve_path=True),
    required=True,
    help="filepath to the directory containing the data to be uploaded",
)
def validate(folderpath: str):
    """
    Validates the sha256 checksum of the sequence data files. This command must be executed before the encryption and upload can start.

    Folderpath: The main path to the directory containing sub directories with the sequence data and metainformation.\n
    Exception: Is raised if an error occurs during the checksum validation.
    """

    log.info("Starting checksum validation...")
    try:
        folderpath = Path(folderpath)
        worker_inst = Worker(folderpath)
        worker_inst.validate_checksum()
        worker_inst.show_summary("SHA256 checksum validation")

    except (KeyboardInterrupt, Exception) as e:
        log.error(format_exc())

    finally:
        if worker_inst.write_progress: write_yaml(worker_inst.progress_file_checksum, worker_inst.get_dict_for_report())
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
@click.option(
    "--pubkey_grz",
    metavar="STRING",
    type=str,
    required=True,
    help="public crypt4gh key of the GRZ",
)
def upload(config, sumission_file, pubkey_grz):
    """
    Uploads a submission file to s3 using the provided configuration.
    Args:
        config (str): The path to the configuration file.
        sumission_file (str): The path to the submission file.
        pubkey_grz (str): The public key for authentication.
    Returns:
        None
    """

    options = {
        "config_file": config,
        "meta_file": sumission_file,
        "public_key": pubkey_grz,
    }

    # TODO: Implement the upload logic


if __name__ == "__main__":
    cli.add_command(validate)
    cli.add_command(encrypt)
    cli.add_command(upload)
    cli()
