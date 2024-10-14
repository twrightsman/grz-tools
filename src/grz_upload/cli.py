"""
CLI module for handling command-line interface operations.
"""
import yaml

''' python modules '''
import logging
import logging.config
from traceback import format_exc

import click

''' package modules '''
from grz_upload.logging_setup import add_filelogger
from grz_upload.parser import Worker

log = logging.getLogger(__name__)


class OrderedGroup(click.Group):
    def list_commands(self, ctx):
        # Return commands in the order they were added
        return list(self.commands.keys())


@click.group(cls=OrderedGroup)
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

    :param log_file: Path to the log file. If provided, a file logger will be added.
    :param log_level: Log level for the logger. It should be one of the following:
                       DEBUG, INFO, WARNING, ERROR, CRITICAL.
    """

    if log_file:
        add_filelogger(log_file, log_level)  # Add file logger
    else:
        logging.basicConfig(
            level=log_level.upper(),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    logging.getLogger(__name__).info("Logging setup complete.")


@cli.command()
@click.option(
    "-s",
    "--submission_dir",
    metavar="STRING",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True, resolve_path=True),
    required=True,
    help="Path to the submission directory containing both metadata and files",
)
@click.option(
    "-w",
    "--working_dir",
    metavar="STRING",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True, writable=True, resolve_path=True),
    required=False,
    default=None,
    callback=lambda c, p, v: v if v else c.params['submission_dir'],
    help="Path to a working directory where intermediate files can be stored",
)
def validate(submission_dir: str, working_dir: str):
    """
    Validates the sha256 checksum of the sequence data files. This command must be executed before the encryption and upload can start.
    """

    log.info("Starting validation...")

    worker_inst = Worker(submission_dir, working_dir)
    worker_inst.validate()

    log.info("Validation done!")



@cli.command()
@click.option(
    "-s",
    "--submission_dir",
    metavar="STRING",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True, resolve_path=True),
    required=True,
    help="Path to the submission directory containing both metadata and files",
)
@click.option(
    "-w",
    "--working_dir",
    metavar="STRING",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True, writable=True, resolve_path=True),
    required=False,
    default=None,
    callback=lambda c, p, v: v if v else c.params['folderpath'],
    help="Path to a working directory where intermediate files can be stored",
)
@click.option(
    "-c",
    "--config_file",
    metavar="STRING",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
    required=True,
    default="~/.config/grz_upload/config.yaml",
    help="Path to config file",
)
# @click.option(
#     "--pubkey_grz",
#     metavar="STRING",
#     type=str,
#     required=False,
#     help="public crypt4gh key of the GRZ",
# )
def encrypt(submission_dir, working_dir, config_file, pubkey_grz):
    """
    Prepares a submission using the provided filepath, metafile, and public key.
    """
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    pubkey_path = config["public_key_path"]

    log.info("Starting encryption...")

    try:
        worker_inst = Worker(submission_dir, working_dir)
        worker_inst.encrypt(pubkey_path)

    except (KeyboardInterrupt, Exception) as e:
        log.error(format_exc())

    finally:
        log.info("Shutting Down - Live long and prosper")
        logging.shutdown()


@cli.command()
@click.option(
    "-s",
    "--submission_dir",
    metavar="STRING",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True, resolve_path=True),
    required=True,
    help="Path to the submission directory containing both metadata and files",
)
@click.option(
    "-w",
    "--working_dir",
    metavar="STRING",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True, writable=True, resolve_path=True),
    required=False,
    default=None,
    callback=lambda c, p, v: v if v else c.params['folderpath'],
    help="Path to a working directory where intermediate files can be stored",
)
@click.option(
    "-c",
    "--config_file",
    metavar="STRING",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
    required=True,
    default="~/.config/grz_upload/config.yaml",
    help="Path to config file",
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
    is_flag=True
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
        "folderpath": folderpath,
        "use_s3cmd": use_s3cmd
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
    cli()
