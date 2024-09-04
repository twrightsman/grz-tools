import sys
import click
import logging
import logging.config

from grz_upload.prepare_file_submission import prepare_file_submission
from grz_upload.upload_file_submission import upload_files

sys.path.append("/home/gorka/Documents/GHGA/grz-upload-client/grz_upload")  # Replace with your project path


@click.group()
@click.version_option(version='0.1', prog_name='grz_upload')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def cli(verbose):
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

@click.command()
@click.option('-c', '--config', metavar='STRING', type=str, required=True,
                                   help='config file containing the required s3 options')
@click.option('-f', '--metafile', metavar='STRING', type=str, required=False, 
              help='metafile in json format for data upload to a GRZ s3 structure')
@click.option('--pubkey_grz', metavar='STRING', type=str, required=True,
                                   help='public crypt4gh key of the GRZ')
def prepare_submission(config, metafile, pubkey_grz):

    options = {
        'config_file': config,
        'meta_file': metafile,
        'public_key': pubkey_grz
    }

    prepare_file_submission(options)


@click.command()
@click.option('-c', '--config', metavar='STRING', type=str, required=True,
                                   help='config file containing the required s3 options')
@click.option('-f', '--sumission-file', metavar='STRING', type=str, required=False, 
              help='metafile in json format for data upload to a GRZ s3 structure')
@click.option('--pubkey_grz', metavar='STRING', type=str, required=True,
                                   help='public crypt4gh key of the GRZ')
def upload(config, sumission_file, pubkey_grz):

    options = {
        'config_file': config,
        'meta_file': sumission_file,
        'public_key': pubkey_grz
    }
    upload_files(options)

if __name__ == '__main__':
    cli.add_command(prepare_submission)
    cli.add_command(upload)
    cli()