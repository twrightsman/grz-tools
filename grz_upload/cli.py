import json
import logging
import logging.config
from argparse import ArgumentParser as ArgumentParser
from argparse import RawDescriptionHelpFormatter
from base64 import b64decode
from datetime import datetime
from os import environ
from pathlib import Path
from subprocess import check_call
from time import sleep
from traceback import format_exc

from yaml import safe_load, YAMLError

from grz_upload.file_operations import calculate_md5
from grz_upload.upload import S3UploadWorker

log = logging.getLogger(__name__)

# logging configuration
_PACKAGE_ROOT = "grz_upload"

_LOGGING_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
_LOGGING_DATEFMT = '%Y-%m-%d %I:%M %p'
_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': _LOGGING_FORMAT,
            'datefmt': _LOGGING_DATEFMT,
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',  # Default is stderr
        },
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['default'],
            'level': 'WARNING',
            'propagate': False
        },
        _PACKAGE_ROOT: {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        },
        '__main__': {  # if __name__ == '__main__'
            'handlers': ['default'],
            'level': 'DEBUG',
            'propagate': False
        },
    }
}


def _add_filelogger(file_path, level="INFO"):
    """
    Add file logging for this package
    """
    package_logger = logging.getLogger(_PACKAGE_ROOT)
    fh = logging.FileHandler(file_path)
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(_LOGGING_FORMAT, _LOGGING_DATEFMT))
    package_logger.addHandler(fh)


class Parser(object):
    def __init__(self):
        self.__parser = ArgumentParser(description="""
        Manages the encryption and upload of files into the s3 structure of a GRZ.
        """, formatter_class=RawDescriptionHelpFormatter)
        self.initialiseParser()
        self.__config_file = ''
        self.__s3_dict = {}
        self.__meta_file = None
        self.__json_file = None
        self.__meta_dict = {}
        self.__json_dict = {}
        self.__pubkey = ''
        self.__file_done = 0
        self.__file_todo = 0
        self.__file_failed = 0
        self.__file_total = 0
        self.__file_invalid = 0
        self.parse()

    def initialiseParser(self):
        self.__parser.add_argument('-c', '--config', metavar='STRING', dest='config', type=str, required=True,
                                   help='config file containing the required s3 options')
        # self.__parser.add_argument('--metafile', metavar = 'STRING', dest = 'metafile', type = str, required = False, help= 'metafile listing the data to be upload into s3 structure')
        self.__parser.add_argument('--metafile', metavar='STRING', dest='jsonfile', type=str, required=False,
                                   help='metafile in json format for data upload to a GRZ s3 structure')
        self.__parser.add_argument('--pubkey_grz', metavar='STRING', dest='pubkey_grz', type=str, required=True,
                                   help='public crypt4gh key of the GRZ')

    def parse(self, inputstring=None):
        if inputstring == None:
            self.__options = self.__parser.parse_args()
        else:
            self.__options = self.__parser.parse_args(inputstring)

    '''
    Method changes the inputstring to an absolute path
    @param filepath: String
    @rtype: Path
    @return: Path
    '''

    def get_absolute_path_pathlib(self, filepath):
        if filepath == '~':
            filepath = environ['HOME']
        elif filepath[0] == '~':
            filepath = filepath.replace(filepath[0], environ['HOME'])
        else:
            filepath = Path(filepath).resolve()
        return Path(filepath)

    '''
    Read a yaml file and store details in a dictionary
    @param filepath: String
    @rtype: dictionary
    @return: dictionary of s3 configurations parameter
    '''

    def read_yaml(self, filepath):
        temp_dict = {}
        with open(str(filepath), 'r', encoding='utf-8') as filein:
            try:
                temp_dict = safe_load(filein)
            except YAMLError:
                temp_dict = {}
                for i in format_exc().split('\n'): log.error(i)
        return temp_dict

    def check_yaml(self):
        temp = ('s3_url', 's3_access_key', 's3_secret', 's3_bucket')
        failed = False
        for i in temp:
            if i not in self.__s3_dict:
                log.error(f'Please provide {i} in {self.__config_file}')
                failed = True
        if 'use_https' in self.__s3_dict:
            if self.__s3_dict['use_https'] and not self.__s3_dict['s3_url'].startswith('https://'): self.__s3_dict[
                's3_url'] = f'https://{self.__s3_dict["s3_url"]}'
        return failed

    def check_public_key(self):
        with open(self.__pubkey, 'rb') as f:
            f.readline()
            key_data = f.readline().strip()
            f.readline()
        try:
            key_data = b64decode(key_data)
        except Exception as e:
            log.error('Public crypt4gh key file of GRZ is not a valid file (--pubkey_grz option)')
            log.error(f"Key decoding error: {e}")
            exit(2)
        # Check if the key is 32 bytes long (Curve25519 public key size)
        if len(key_data) != 32:
            log.error('Public crypt4gh key file of GRZ is not a valid file (--pubkey_grz option)')
            log.error('Client public key must be a 32 bytes long bytes sequence')
            exit(2)

    def build_dict(self, filename, fullpath, checksum, checksum_enc, upload_status):
        temp = {}
        temp['file_id'] = filename
        temp['file_location'] = fullpath
        temp['original_md5'] = checksum
        temp['encrypted_md5'] = checksum_enc
        temp['upload_status'] = upload_status
        temp['filename_encrypted'] = filename if filename.endswith(S3UploadWorker.EXT) else filename + S3UploadWorker.EXT
        return temp

    def check_json(self):
        try:
            with open(str(self.__json_file), 'r', encoding='utf-8') as jsonfile:
                self.__json_dict = json.load(jsonfile)
        except json.JSONDecodeError:
            log.error(
                f'The provided file at {self.__json_file} is not a valid JSON (option --metafile).')
            exit()

        stop = False
        for donor in self.__json_dict.get("Donors", {}):
            for lab_data in donor.get("LabData", {}):
                for sequence_data in lab_data.get("SequenceData", {}):
                    for files_data in sequence_data.get("files", {}):
                        files_invalid = self.__file_invalid
                        self.__file_total += 1
                        filename = files_data['filename']
                        filepath = files_data['filepath']
                        filechecksum = files_data['fileChecksum']
                        filechecksum_enc = files_data.get('fileChecksum_encrypted', '')
                        upload_status = files_data.get('upload_status', 'in progress')
                        if upload_status == 'failed': self.__file_failed += 1
                        fullpath = Path(filepath) / filename
                        if upload_status == 'finished':
                            log.warning(f'File ID {filename} already scanned in metafile - Skipping')
                            self.__file_done += 1
                            self.__meta_dict[filename] = self.build_dict(filename, fullpath, filechecksum,
                                                                         filechecksum_enc, upload_status)
                            continue
                        log.info(f'Preprocessing: {fullpath}')
                        if not fullpath.is_file():
                            log.error(
                                f'The provided file {filename} in {self.__json_file} does not exist.')
                            self.__file_invalid += 1
                            stop = True
                        filechecksum_calc = calculate_md5(fullpath)
                        if filechecksum == filechecksum_calc:
                            log.info(f'Preprocessing: {fullpath} - md5sum correct')
                        else:
                            log.error(
                                f'Preprocessing: {fullpath} - md5sum incorrect - provided: {filechecksum} - calculated: {filechecksum_calc}')
                            if self.__file_invalid == files_invalid: self.__file_invalid += 1
                            stop = True
                        self.__meta_dict[filename] = self.build_dict(filename, fullpath, filechecksum, filechecksum_enc,
                                                                     upload_status)
                        self.__file_todo += 1

        if stop:
            log.error('Please tend to the errors listed above and restart the script afterwards!')
            exit(2)

        if self.__file_todo == 0:
            log.warning('All files in the metafile have been already processed')
            exit(1)

    def show_information(self, logfile):
        log.info(f's3 config file {self.__config_file}')
        # log.info(f'meta file: {self.__meta_file}')
        log.info(f'meta file: {self.__json_file}')
        log.info(f'GRZ public crypt4gh key: {self.__pubkey}')
        log.info(f'log file: {logfile}')
        log.info(f'total files in metafile: {self.__file_total}')
        log.info(f'uploaded files: {self.__file_done}')
        log.info(f'failed files: {self.__file_failed}')
        log.info(f'invalid files: {self.__file_invalid}')
        log.info(f'waiting files: {self.__file_todo}')

    def main(self):
        self.__config_file = self.get_absolute_path_pathlib(self.__options.config)
        if not self.__config_file.is_file():
            log.error('Please provide a valid path to the config file (-c/--config option)')
            exit(2)
        self.__s3_dict = self.read_yaml(self.__config_file)
        if self.check_yaml(): exit(2)

        self.__pubkey = self.get_absolute_path_pathlib(self.__options.pubkey_grz)
        if not self.__pubkey.is_file():
            log.error('Please provide a valid path to the public cryp4gh key (--pubkey_grz)')
            exit(2)
        self.check_public_key()

        self.__json_file = self.get_absolute_path_pathlib(self.__options.jsonfile)
        if not self.__json_file.is_file():
            log.error('Please provide a valid path to the meta file (--metafile)')
            exit(2)
        self.check_json()

    def get_json_dict(self):
        return self.__json_dict

    def get_json_file(self):
        return self.__json_file

    def get_meta_dict(self):
        return self.__meta_dict

    def get_meta_file(self):
        return self.__meta_file

    def get_s3_dict(self):
        return self.__s3_dict

    def get_pubkey_grz(self):
        return self.__pubkey

    s3_dict = property(get_s3_dict)
    json_dict = property(get_json_dict)
    json_file = property(get_json_file)
    meta_dict = property(get_meta_dict)
    meta_file = property(get_meta_file)
    pubkey_grz = property(get_pubkey_grz)


def main():
    mainlog, logfile, logfile_gz, parser, worker = None, None, None, None, None
    try:
        logging.config.dictConfig(_LOGGING_CONFIG)

        parser = Parser()
        parser.main()

        logfile = parser.get_absolute_path_pathlib(
            (f'{datetime.today().strftime("%Y%m%d-%H%M")}.{_PACKAGE_ROOT}.txt'))
        logfile_gz = logfile.with_suffix(logfile.suffix + '.gz')
        _add_filelogger(str(logfile))

        parser.show_information(logfile_gz)
        sleep(2)

        # worker_inst = Worker(mainlog, parser_inst.meta_dict, parser_inst.meta_file, parser_inst.s3_dict, parser_inst.pubkey_grz)
        worker = S3UploadWorker(parser.meta_dict, parser.json_file, parser.json_dict,
                                parser.s3_dict, parser.pubkey_grz)
        worker.main()

    except (KeyboardInterrupt, Exception) as e:
        log.error(format_exc())
    finally:
        if worker is not None:
            # worker_inst.write_csv()
            worker.update_json()
            worker.write_json()
            worker.show_information()
        log.info('Shutting Down - Live long and prosper')

        if logfile is not None:
            check_call(['gzip', str(logfile)])
            logfile_gz.chmod(0o664)

        logging.shutdown()


if __name__ == '__main__':
    main()
