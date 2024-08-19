import unittest
from unittest.mock import patch, MagicMock
import io
from encrypt_upload import prepare_header, encrypt_part, stream_encrypt_and_upload

class TestEncryptUpload(unittest.TestCase):

    @patch('your_module.boto3.client')
    def test_stream_encrypt_and_upload(self, mock_boto_client):
        # Set up
        mock_s3_client = mock_boto_client.return_value
        mock_s3_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
        mock_s3_client.upload_part.return_value = {'ETag': 'test-etag'}

        # Call function
        keys = (0, b'seckey', b'pubkey')
        header_info = prepare_header(keys)
        original_md5, encrypted_md5 = stream_encrypt_and_upload(
            'test_file.txt', 'test_file_id', keys, mock_s3_client, 'test-bucket', 'test_log.log'
        )

        # Assertions
        mock_s3_client.create_multipart_upload.assert_called_once_with(Bucket='test-bucket', Key='test_file_id')
        self.assertEqual(len(mock_s3_client.upload_part.call_args_list), 1)
        mock_s3_client.complete_multipart_upload.assert_called_once()

    def test_encrypt_part(self):
        session_key = b'session_key'
        data = b'1234567890' * 65536  # 640 KB of data
        encrypted_data = encrypt_part(data, session_key)
        self.assertTrue(len(encrypted_data) > len(data))

    def test_prepare_header(self):
        keys = ((0, b'seckey', b'pubkey'),)
        header, session_key, keys = prepare_header(keys)
        self.assertTrue(len(header) > 0)
        self.assertEqual(len(session_key), 32)

if __name__ == '__main__':
    unittest.main()
