import logging
import logging.config

from datetime import datetime

from grz_upload.parser import Parser
from src.grz_upload.uploader import S3UploadWorker

log = logging.getLogger(__name__)

def upload_files(options):

        loggername = 'grz_upload'

        parser = Parser()
        parser.set_options(options)
        parser.main()
        parser.show_information()
        
        logfile = parser.get_absolute_path_pathlib((f'{datetime.today().strftime("%Y%m%d-%H%M")}.{loggername}.txt'))
        logfile_gz = logfile.with_suffix(logfile.suffix + '.gz')

        parser.show_information(logfile_gz)
        
        uploader = S3UploadWorker(parser.s3_dict, parser.pubkey_grz)
        
        print('----------')
        print(parser.s3_dict)
        uploader.encrypt_upload_files()
        