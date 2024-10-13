from grz_upload.parser import SubmissionMetadata


def test_SubmissionMetadata(temp_metadata_file_path):
    submission_metadata = SubmissionMetadata(temp_metadata_file_path)

    submission_metadata.files

