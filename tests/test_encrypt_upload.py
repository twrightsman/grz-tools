import unittest
from unittest.mock import patch, mock_open, MagicMock
import hashlib
import os
from my_package.encrypt_upload import encrypt_and_upload_files

class TestEncryptUpload(unittest.TestCase):

    @patch('my_package.encrypt_upload.boto3.client')
    @patch('builtins.open', new_callable=mock_open, read_data=b'This is a test file.')
    @patch('crypt4gh.keys.get_public_key')
    def test_encrypt_and_upload_files(self, mock_get_public_key, mock_open_file, mock_boto_client):
        # Mock the S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Mock the public key
        mock_get_public_key.return_value = b'mock_public_key'

        # Mock configuration
        metadata_file_path = 'test_metadata.csv'
        public_key_path = 'mock_public_key_path'
        s3_bucket = 'mock_s3_bucket'

        # Create a mock metadata file
        metadata_content = 'file_path,s3_key\n/path/to/file1,s3_key1\n/path/to/file2,s3_key2\n'
        with patch('builtins.open', new_callable=mock_open, read_data=metadata_content) as mock_metadata_file:
            # Run the function
            encrypt_and_upload_files(metadata_file_path, public_key_path, s3_bucket)

            # Ensure files are opened correctly
            mock_open_file.assert_called_with('/path/to/file1', 'rb')

            # Ensure S3 upload is called
            self.assertTrue(mock_s3_client.upload_fileobj.called)

    def test_md5_checksum(self):
        # Create a test file content
        test_content = b'This is a test file.'

        # Calculate MD5 manually
        md5_hash = hashlib.md5()
        md5_hash.update(test_content)
        expected_md5 = md5_hash.hexdigest()

        # Write content to a temporary file
        temp_file_path = 'temp_test_file'
        with open(temp_file_path, 'wb') as f:
            f.write(test_content)

        # Read file and calculate MD5 using the script's function
        calculated_md5 = hashlib.md5()
        with open(temp_file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                calculated_md5.update(chunk)

        os.remove(temp_file_path)

        self.assertEqual(calculated_md5.hexdigest(), expected_md5)

if __name__ == '__main__':
    unittest.main()