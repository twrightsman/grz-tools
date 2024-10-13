from grz_upload.parser import SubmissionMetadata


def test_SubmissionMetadata(temp_metadata_file_path):
    submission_metadata = SubmissionMetadata(temp_metadata_file_path)

    errors = list(submission_metadata.validate())
    assert errors == []

    assert len(submission_metadata.files) > 0

# TODO: test encrypt submission
# TODO: test upload submission