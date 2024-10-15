# GRZ Upload

A tool to encrypt and upload files to S3 with MD5 checksum logging.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Command-Line Interface](#command-line-interface)
  - [Configuration](#configuration)
- [Example](#example)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)

## Introduction

This tool provides a streamlined way to encrypt files using the `crypt4gh` library, calculate MD5 checksums for both the original and encrypted files, and upload the encrypted files to an S3 bucket. It also logs the progress and outcomes of these operations in a metadata file.

## Features

- **File Encryption**: Encrypt files using `crypt4gh`.
- **MD5 Checksum Calculation**: Calculate and log MD5 checksums for both original and encrypted files.
- **S3 Upload**: Upload encrypted files directly to an S3 bucket.
- **Progress Logging**: Log the progress and results of operations in a metadata CSV file.

## Installation

To install this package, download the `grz_upload.zip` file and install it using `pip`:

```bash
pip install grz_upload.zip
```

to test edits to the code, you can use

```bash
pip install -e grz_upload.zip
```

## Usage

### Command-Line Interface

The tool can be run from the command line using the `encrypt-upload` command. Ensure you have your configuration file ready.

```bash
encrypt-upload --config path/to/config.yaml
```

### Configuration

The configuration file (in YAML format) should include the following parameters:

- `metadata_file_path`: Path to the metadata CSV file containing file details.
- `public_key_path`: Path to the grz public key used for encryption.
- `s3_url`: Address for your S3 endpoint
- `s3_access_key`: Users access key for S3
- `s3_secret`: Users secret for S3
- `s3_bucket`: Name of the S3 bucket where encrypted files will be uploaded.

Example `config.yaml`:

```yaml
metadata_file_path: 'path/to/metadata_file.csv'
public_key_path: 'path/to/public/key'
s3_url: 'your-s3-url'
s3_bucket: 'your-s3-bucket-name'
s3_access_key: 'your-s3-bucket-key'
s3_secret: 'your-s3-bucket-secret'
```

## Example

Here's an example of how to use this tool:

1. Prepare your `config.yaml` file with the appropriate paths and S3 bucket name.
2. Ensure your metadata CSV file is correctly formatted with at least `File id` and `File Location` columns.
3. Run the tool using the command:

```bash
encrypt-upload --config path/to/config.yaml
```

This will start the encryption and upload process, logging progress in the metadata file.

## Testing

To run the tests, navigate to the root directory of your project and execute:

```bash
python -m unittest discover tests
```

This will discover and run all the test cases in the `tests` directory.

## Project Structure

```
grz_upload/
│
├── grz_upload/
│   ├── __init__.py
│   ├── encrypt_upload.py
│   ├── config.yaml
│
├── tests/
│   ├── __init__.py
│   └── test_encrypt_upload.py
│
├── pyproject.toml
├── README.md
└── LICENSE
```

## Contributing

<!-- Add details about how others can contribute to the project -->

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgements

parts of cryp4gh code is used in modified form

# To Do
- logging not working properly
- check if file id has extension, add correct extension and .c4gh
- catch user interruption and kill multipart upload
- check for all open multipart uploads and clean them
