# OpenS3 SDK

[![Python 3.6+](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://badge.fury.io/py/openS3-sdk.svg)](https://badge.fury.io/py/openS3-sdk)

A boto3-compatible Python SDK for interacting with OpenS3, a local implementation of Amazon S3-like object storage functionality. This SDK provides a familiar interface for developers used to AWS boto3, making it easy to transition between AWS S3 and OpenS3.

> **⚠️ WARNING: You must have the OpenS3-server set up and running for this SDK to work properly.**
> Please refer to the [OpenS3-server repository](https://github.com/SourceBox-LLC/OpenS3-server) for server setup instructions.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Client Configuration](#client-configuration)
- [Bucket Operations](#bucket-operations)
- [Object Operations](#object-operations)
- [Error Handling](#error-handling)
- [Response Structure](#response-structure)
- [Examples](#examples)
- [Development](#development)
- [Compatibility with boto3](#compatibility-with-boto3)
- [License](#license)

## Features

- **Boto3-compatible Interface**: Familiar API for AWS developers
- **Bucket Operations**: Create, list, and delete buckets
- **Object Operations**: Upload, download, list, and delete objects
- **File Utilities**: Convenient methods for file uploads and downloads
- **Flexible Authentication**: Support for both AWS-style credentials and basic auth tuples
- **Consistent Responses**: Response structures match boto3 format

## Installation

### From PyPI (Recommended)

```bash
# Install from PyPI
pip install openS3-sdk
```

### From Source

```bash
# Clone the repository
git clone https://github.com/SourceBox-LLC/OpenS3-SDK.git
cd opens3-sdk

# Create and activate a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install the SDK in development mode
pip install -e .
```

### From GitHub

```bash
pip install git+https://github.com/SourceBox-LLC/OpenS3-SDK.git
```

**Note**: While the package name on PyPI is `openS3-sdk`, you'll still import it using `import opens3` in your code.

## Quick Start

```python
import opens3

# Initialize a client
s3 = opens3.client('s3', 
                  endpoint_url='http://localhost:8000',
                  aws_access_key_id='admin',
                  aws_secret_access_key='password')

# Create a bucket
s3.create_bucket(Bucket='my-bucket')

# Upload a file
s3.upload_file('local_file.txt', 'my-bucket', 'remote_file.txt')

# Download a file
s3.download_file('my-bucket', 'remote_file.txt', 'downloaded_file.txt')
```

## Client Configuration

### Initialization

The OpenS3 client can be initialized with various configurations:

```python
import opens3

# Basic initialization with defaults (uses admin/password auth)
s3 = opens3.client('s3', endpoint_url='http://localhost:8000')

# Using environment variables (recommended)
# Set OPENS3_ACCESS_KEY and OPENS3_SECRET_KEY environment variables
s3 = opens3.client('s3', endpoint_url='http://localhost:8000')

# Explicit credential parameters
s3 = opens3.client('s3',
                  endpoint_url='http://localhost:8000',
                  aws_access_key_id='admin',  # Also supports: access_key
                  aws_secret_access_key='password')  # Also supports: secret_key

# Direct auth tuple
s3 = opens3.client('s3',
                  endpoint_url='http://localhost:8000',
                  auth=('admin', 'password'))
```

### Configuration Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `endpoint_url` | `str` | URL to the OpenS3 server | `'http://localhost:8000'` |
| `access_key` | `str` | Username for authentication (from `OPENS3_ACCESS_KEY` env var) | `'admin'` |
| `secret_key` | `str` | Password for authentication (from `OPENS3_SECRET_KEY` env var) | `'password'` |
| `aws_access_key_id` | `str` | Alternate parameter name for username | `None` |
| `aws_secret_access_key` | `str` | Alternate parameter name for password | `None` |
| `auth` | `tuple` | Direct auth tuple `(username, password)` | `None` |

## Bucket Operations

### Create a Bucket

```python
response = s3.create_bucket(Bucket='my-bucket')
```

**Parameters:**
- `Bucket`: (Required) Name of the bucket to create

**Response:**
```python
{
    'ResponseMetadata': {
        'HTTPStatusCode': 201
    },
    'Location': '/my-bucket'
}
```

### List Buckets

```python
response = s3.list_buckets()
```

**Response:**
```python
{
    'Buckets': [
        {
            'Name': 'my-bucket',
            'CreationDate': datetime.datetime(2025, 5, 12, 17, 0, 0)
        },
        # More buckets...
    ],
    'Owner': {'ID': 'admin'}
}
```

### Check if a Bucket Exists

```python
response = s3.head_bucket(Bucket='my-bucket')
```

**Parameters:**
- `Bucket`: (Required) Name of the bucket to check

**Returns:**
- `True` if the bucket exists and the caller has permission to access it
- `False` if the bucket does not exist

**Raises:**
- `HTTPError` if the caller does not have permission to access the bucket (403) or other errors

**Example:**

```python
try:
    if s3.head_bucket('my-bucket'):
        print("Bucket exists and you have access")
    else:
        print("Bucket does not exist")
except Exception as e:
    print(f"Error checking bucket: {e}")
```

### Delete a Bucket

```python
response = s3.delete_bucket(Bucket='my-bucket')
```

**Parameters:**
- `Bucket`: (Required) Name of the bucket to delete

**Response:**
```python
{
    'ResponseMetadata': {
        'HTTPStatusCode': 200
    }
}
```

## Object Operations

### Upload an Object

#### Using put_object (Memory)

```python
response = s3.put_object(
    Bucket='my-bucket',
    Key='hello.txt',
    Body=b'Hello World!'
)
```

**Parameters:**
- `Bucket`: (Required) Name of the bucket
- `Key`: (Required) Object key name
- `Body`: (Required) Object data - can be bytes or a file-like object

**Response:**
```python
{
    'ResponseMetadata': {
        'HTTPStatusCode': 201
    },
    'ETag': '"fake-etag"'
}
```

#### Using upload_file (File)

```python
response = s3.upload_file('local_file.txt', 'my-bucket', 'remote_file.txt')
```

**Parameters:**
- `Filename`: (Required) Path to the local file
- `Bucket`: (Required) Name of the bucket
- `Key`: (Required) Object key name in the bucket

### List Objects

```python
response = s3.list_objects_v2(
    Bucket='my-bucket',
    Prefix='folder/'  # Optional prefix
)
```

**Parameters:**
- `Bucket`: (Required) Name of the bucket
- `Prefix`: (Optional) Limit results to keys beginning with this prefix

**Response:**
```python
{
    'Contents': [
        {
            'Key': 'folder/file1.txt',
            'LastModified': datetime.datetime(2025, 5, 12, 17, 0, 0),
            'Size': 12,
            'ETag': '"fake-etag"',
            'StorageClass': 'STANDARD'
        },
        # More objects...
    ],
    'Name': 'my-bucket',
    'Prefix': 'folder/',
    'MaxKeys': 1000,
    'KeyCount': 1,
    'IsTruncated': False
}
```

### Download an Object

#### Using get_object (Memory)

```python
response = s3.get_object(Bucket='my-bucket', Key='hello.txt')
content = response['Body'].content  # Access the binary content
text = content.decode('utf-8')      # Convert to text if needed
```

**Parameters:**
- `Bucket`: (Required) Name of the bucket
- `Key`: (Required) Object key name

**Response:**
```python
{
    'Body': <Response object>,
    'ContentLength': 12,
    'LastModified': datetime.datetime(2025, 5, 12, 17, 0, 0),
    'ContentType': 'text/plain'
}
```

#### Using download_file (File)

```python
response = s3.download_file('my-bucket', 'hello.txt', 'downloaded.txt')
```

**Parameters:**
- `Bucket`: (Required) Name of the bucket
- `Key`: (Required) Object key name
- `Filename`: (Required) Path where the file should be saved

### Delete an Object

```python
response = s3.delete_object(Bucket='my-bucket', Key='hello.txt')
```

**Parameters:**
- `Bucket`: (Required) Name of the bucket
- `Key`: (Required) Object key name

**Response:**
```python
{
    'ResponseMetadata': {
        'HTTPStatusCode': 200
    }
}
```

## Error Handling

The SDK provides boto3-compatible error handling with enhanced error messages. Starting from version 0.1.6, the SDK includes detailed error information from the server to help with debugging and user feedback.

### Basic Error Handling

```python
from opens3.exceptions import ClientError, NoSuchBucket, NoSuchKey

try:
    s3.get_object(Bucket='non-existent-bucket', Key='file.txt')
except NoSuchBucket as e:
    print(f"Bucket does not exist: {e}")
except NoSuchKey as e:
    print(f"Object does not exist: {e}")
except ClientError as e:
    print(f"Error: {e.response['Error']['Message']}")
```

### Enhanced Error Details (v0.1.6+)

In version 0.1.6 and above, HTTP errors include additional details extracted from the server response:

```python
import requests

try:
    # Try to delete a non-empty bucket
    s3.delete_bucket(Bucket='my-bucket-with-objects')
except requests.exceptions.HTTPError as e:
    # Access detailed error message from the server
    if hasattr(e, 'detail'):
        print(f"Detailed error: {e.detail}")
    # Access the original status code
    if hasattr(e, 'status_code'):
        print(f"Status code: {e.status_code}")
    # General error handling
    print(f"Error: {str(e)}")
```

This provides more specific information about why operations failed, such as detailed reasons for bucket deletion failures.

## Response Structure

OpenS3 SDK responses mirror the structure of boto3 responses to provide compatibility. Here's a breakdown of common response elements:

- `ResponseMetadata`: Contains metadata about the request, including `HTTPStatusCode`
- Operation-specific data (e.g., `Buckets` for `list_buckets`, `Contents` for `list_objects_v2`)

## Examples

### Working with Buckets

```python
import opens3

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')

# Create multiple buckets
s3.create_bucket(Bucket='bucket1')
s3.create_bucket(Bucket='bucket2')

# List all buckets
response = s3.list_buckets()
for bucket in response['Buckets']:
    print(f"Bucket: {bucket['Name']}, Created: {bucket['CreationDate']}")

# Clean up - delete buckets
s3.delete_bucket(Bucket='bucket1')
s3.delete_bucket(Bucket='bucket2')
```

### Working with Objects

```python
import opens3

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')

# Create a bucket for our objects
s3.create_bucket(Bucket='files')

# Upload multiple objects
s3.put_object(Bucket='files', Key='doc1.txt', Body=b'Document 1 content')
s3.put_object(Bucket='files', Key='doc2.txt', Body=b'Document 2 content')
s3.put_object(Bucket='files', Key='folder/doc3.txt', Body=b'Document 3 content')

# List all objects
response = s3.list_objects_v2(Bucket='files')
print(f"All objects in bucket 'files':")
for obj in response['Contents']:
    print(f"- {obj['Key']} ({obj['Size']} bytes)")

# List objects with prefix
response = s3.list_objects_v2(Bucket='files', Prefix='folder/')
print(f"\nObjects in 'folder/':")
for obj in response['Contents']:
    print(f"- {obj['Key']}")

# Download an object
response = s3.get_object(Bucket='files', Key='doc1.txt')
print(f"\nContent of doc1.txt: {response['Body'].content.decode('utf-8')}")
```

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest

# Run the test suite
python -m pytest
```

### Building the Package

```bash
pip install build twine
python -m build
```

## Compatibility with boto3

This SDK aims to provide a compatible interface with boto3 for the most common S3 operations. It's designed to make it easy for developers to switch between AWS S3 and OpenS3 with minimal code changes.

Some notable differences:

- Only core S3 operations are currently supported
- Some advanced features like multipart uploads and presigned URLs are not yet implemented
- The resource interface (`opens3.resource()`) is not yet available

## License

MIT