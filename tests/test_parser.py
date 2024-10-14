from grz_upload.parser import EncryptedSubmission, SubmissionMetadata
from pathlib import Path


def test_SubmissionMetadata(temp_metadata_file_path):
    submission_metadata = SubmissionMetadata(temp_metadata_file_path)

    errors = list(submission_metadata.validate())
    assert errors == []

    assert len(submission_metadata.files) > 0

def test_EncryptedSubmission():
    input_path = "/submission/files/a.fastq"

    for i in (input_path, Path(input_path)):
        enc_path = EncryptedSubmission.get_encrypted_file_path(i)
        assert enc_path == Path(input_path + ".c4gh")
        enc_path = EncryptedSubmission.get_encryption_header_path(input_path)
        assert enc_path == Path(input_path + ".c4gh_header")

# TODO: test encrypt submission
# TODO: test upload submission
