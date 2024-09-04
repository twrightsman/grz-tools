import logging
import logging.config

from datetime import datetime
from subprocess import check_call
from traceback import format_exc

from grz_upload.constants import _PACKAGE_ROOT, _LOGGING_CONFIG, _LOGGING_FORMAT, _LOGGING_DATEFMT
from grz_upload.parser import Parser

log = logging.getLogger(__name__)


def _add_filelogger(file_path, level="INFO"):
    """
    Add file logging for this package
    """
    package_logger = logging.getLogger(_PACKAGE_ROOT)
    fh = logging.FileHandler(file_path)
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(_LOGGING_FORMAT, _LOGGING_DATEFMT))
    package_logger.addHandler(fh)

def prepare_file_submission(options):
    """
    Prepare the file submission for the GRZ
    """
    log.info('preparing submission...')
    logfile, logfile_gz, parser = None, None, None
    try:
        logging.config.dictConfig(_LOGGING_CONFIG)

        parser = Parser()
        parser.set_options(options)
        parser.main()

        logfile = parser.get_absolute_path_pathlib(
            (f'{datetime.today().strftime("%Y%m%d-%H%M")}.{_PACKAGE_ROOT}.txt'))
        logfile_gz = logfile.with_suffix(logfile.suffix + '.gz')
        _add_filelogger(str(logfile))

        parser.show_information(logfile_gz)
        log.info("The file submission has been prepared.")
        """
        # worker_inst = Worker(mainlog, parser_inst.meta_dict, parser_inst.meta_file, parser_inst.s3_dict, parser_inst.pubkey_grz)
        worker = S3UploadWorker(parser.meta_dict, parser.json_file, parser.json_dict,
                                parser.s3_dict, parser.pubkey_grz)
        worker.main()
        """
    except (KeyboardInterrupt, Exception) as e:
        log.error(format_exc())
    finally:
        parser.create_submission()
        log.info('Shutting Down - Live long and prosper')

        if logfile is not None:
            check_call(['gzip', str(logfile)])
            logfile_gz.chmod(0o664)

        logging.shutdown()