# OpenS3 SDK Cookbook

This cookbook provides practical recipes for common tasks using the OpenS3 SDK. Each recipe demonstrates a specific use case with working code examples.

## Table of Contents

- [Basic Recipes](#basic-recipes)
  - [Setting Up the Client](#setting-up-the-client)
  - [Working with Buckets](#working-with-buckets)
  - [Working with Objects](#working-with-objects)
- [Intermediate Recipes](#intermediate-recipes)
  - [Directory Operations](#directory-operations)
  - [Batch Operations](#batch-operations)
  - [Error Handling](#error-handling)
- [Advanced Recipes](#advanced-recipes)
  - [Custom Metadata](#custom-metadata)
  - [Streaming Data](#streaming-data)
  - [Concurrent Operations](#concurrent-operations)

## Basic Recipes

### Setting Up the Client

#### Basic Connection

```python
import opens3

# Connect with default credentials
s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
```

#### Connection with Custom Credentials

```python
import opens3

# Connect with explicit credentials
s3 = opens3.client('s3',
                  endpoint_url='http://localhost:8000',
                  access_key='admin',  # Uses OPENS3_ACCESS_KEY env var if not specified
                  secret_key='password')  # Uses OPENS3_SECRET_KEY env var if not specified

# For backward compatibility, AWS-style parameters are also supported
s3 = opens3.client('s3',
                  endpoint_url='http://localhost:8000',
                  aws_access_key_id='admin',
                  aws_secret_access_key='password')
```

#### Connection with Environment Variables

```python
import opens3
import os

# Set environment variables
os.environ['OPENS3_ACCESS_KEY'] = 'admin'
os.environ['OPENS3_SECRET_KEY'] = 'password'

# Connect using environment variables
s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
```

#### Connection with Timeout Configuration

```python
import opens3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Create a session with retry logic
session = requests.Session()
retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

# Use the custom session
s3 = opens3.client('s3',
                  endpoint_url='http://localhost:8000',
                  auth=('admin', 'password'))
# Note: Currently the SDK doesn't directly support session customization,
# but this approach would work with future enhancements
```

### Working with Buckets

#### Create and List Buckets

```python
import opens3

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')

# Create buckets for different purposes
s3.create_bucket(Bucket='logs')
s3.create_bucket(Bucket='uploads')
s3.create_bucket(Bucket='backups')

# List all buckets
response = s3.list_buckets()
print("Available buckets:")
for bucket in response['Buckets']:
    print(f"- {bucket['Name']} (created: {bucket['CreationDate']})")
```

#### Check If a Bucket Exists

```python
import opens3
from requests.exceptions import HTTPError

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')

bucket_name = 'my-test-bucket'

# Method 1: Using the new head_bucket method (Recommended)
def check_bucket_exists(client, bucket_name):
    """Check if a bucket exists using head_bucket"""
    try:
        return client.head_bucket(bucket_name)
    except HTTPError as e:
        # If it's a permission error (403), log and re-raise
        if hasattr(e, 'response') and e.response.status_code == 403:
            print(f"Permission denied for bucket: {bucket_name}")
            raise
        # For other errors, re-raise
        raise

# Method 2: Using list_objects_v2 as a fallback
def bucket_exists(client, bucket_name):
    """Check if a bucket exists using list_objects_v2"""
    try:
        # Try to list objects (will fail if bucket doesn't exist)
        client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        return True
    except Exception as e:
        # Check if the error is due to NoSuchBucket
        if hasattr(e, 'response') and getattr(e.response, 'status_code', 0) == 404:
            return False
        # If it's a different error, re-raise it
        raise

# Example usage
try:
    if check_bucket_exists(s3, bucket_name):
        print(f"Bucket {bucket_name} exists")
    else:
        print(f"Bucket {bucket_name} doesn't exist, creating it...")
        s3.create_bucket(Bucket=bucket_name)
except Exception as e:
    print(f"Error checking bucket: {e}")
```

#### Delete All Buckets

```python
import opens3

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')

# List all buckets
response = s3.list_buckets()

# Delete each bucket
for bucket in response['Buckets']:
    bucket_name = bucket['Name']
    print(f"Deleting bucket: {bucket_name}")
    
    # First delete all objects in the bucket
    obj_response = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in obj_response:
        for obj in obj_response['Contents']:
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
    
    # Now delete the empty bucket
    s3.delete_bucket(Bucket=bucket_name)

print("All buckets deleted")
```

### Working with Objects

#### Upload Different Types of Content

```python
import opens3
import io
import json

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
bucket_name = 'content-bucket'

# Create a bucket
s3.create_bucket(Bucket=bucket_name)

# 1. Upload a string
text_content = "This is a simple text file"
s3.put_object(Bucket=bucket_name, Key='text-file.txt', Body=text_content.encode('utf-8'))

# 2. Upload binary data
binary_data = b'\x00\x01\x02\x03\x04\x05'
s3.put_object(Bucket=bucket_name, Key='binary-file.bin', Body=binary_data)

# 3. Upload JSON data
data = {
    'name': 'John Doe',
    'age': 30,
    'email': 'john@example.com'
}
json_data = json.dumps(data).encode('utf-8')
s3.put_object(Bucket=bucket_name, Key='data.json', Body=json_data)

# 4. Upload from a file-like object
file_like_object = io.BytesIO(b'Content from file-like object')
s3.put_object(Bucket=bucket_name, Key='from-memory.txt', Body=file_like_object)

# 5. Upload a local file
s3.upload_file('local-file.txt', bucket_name, 'uploaded-file.txt')

print("All content uploaded successfully")
```

#### List and Filter Objects

```python
import opens3

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
bucket_name = 'documents'

# Create a bucket and add some objects
s3.create_bucket(Bucket=bucket_name)
s3.put_object(Bucket=bucket_name, Key='docs/file1.txt', Body=b'Content 1')
s3.put_object(Bucket=bucket_name, Key='docs/file2.txt', Body=b'Content 2')
s3.put_object(Bucket=bucket_name, Key='images/image1.jpg', Body=b'Image data 1')
s3.put_object(Bucket=bucket_name, Key='images/image2.jpg', Body=b'Image data 2')

# List all objects
response = s3.list_objects_v2(Bucket=bucket_name)
print("All objects:")
for obj in response.get('Contents', []):
    print(f"- {obj['Key']} ({obj['Size']} bytes)")

# List only documents
response = s3.list_objects_v2(Bucket=bucket_name, Prefix='docs/')
print("\nDocument objects:")
for obj in response.get('Contents', []):
    print(f"- {obj['Key']}")

# List only images
response = s3.list_objects_v2(Bucket=bucket_name, Prefix='images/')
print("\nImage objects:")
for obj in response.get('Contents', []):
    print(f"- {obj['Key']}")
```

#### Download and Process Objects

```python
import opens3
import os

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
bucket_name = 'data-bucket'

# Create a bucket and upload a file
s3.create_bucket(Bucket=bucket_name)
s3.put_object(Bucket=bucket_name, Key='numbers.txt', Body=b'1\n2\n3\n4\n5')

# Method 1: Download to memory and process
response = s3.get_object(Bucket=bucket_name, Key='numbers.txt')
content = response['Body'].content.decode('utf-8')

# Process the content in memory
numbers = [int(line) for line in content.strip().split('\n')]
total = sum(numbers)
print(f"Sum of numbers: {total}")

# Method 2: Download to a file and process
download_path = 'downloaded-numbers.txt'
s3.download_file(bucket_name, 'numbers.txt', download_path)

# Process the file
with open(download_path, 'r') as f:
    numbers = [int(line) for line in f.read().strip().split('\n')]
    product = 1
    for num in numbers:
        product *= num
    print(f"Product of numbers: {product}")

# Clean up
os.remove(download_path)
```

## Intermediate Recipes

### Directory Operations

#### Emulating Directories

```python
import opens3
import os

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
bucket_name = 'file-system'

# Create a bucket
s3.create_bucket(Bucket=bucket_name)

# Create a directory structure
directories = [
    'users/',
    'users/john/',
    'users/mary/',
    'projects/',
    'projects/project1/',
    'projects/project2/'
]

# S3 doesn't have real directories, so we create empty objects with directory names
for directory in directories:
    s3.put_object(Bucket=bucket_name, Key=directory, Body=b'')

# Add some files
s3.put_object(Bucket=bucket_name, Key='users/john/profile.json', Body=b'{"name": "John"}')
s3.put_object(Bucket=bucket_name, Key='users/mary/profile.json', Body=b'{"name": "Mary"}')
s3.put_object(Bucket=bucket_name, Key='projects/project1/readme.md', Body=b'# Project 1')

# List a directory
def list_directory(bucket, prefix):
    """List objects in a directory (prefix)"""
    if not prefix.endswith('/'):
        prefix += '/'
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    
    directories = []
    files = []
    
    # Get common prefixes (directories)
    for common_prefix in response.get('CommonPrefixes', []):
        dir_name = common_prefix['Prefix'].split('/')[-2] + '/'
        directories.append(dir_name)
    
    # Get files (objects not ending with /)
    for content in response.get('Contents', []):
        key = content['Key']
        if not key.endswith('/'):
            file_name = key.split('/')[-1]
            files.append(file_name)
    
    return directories, files

# List the users directory
dirs, files = list_directory(bucket_name, 'users')
print("Directories in users/:")
for dir_name in dirs:
    print(f"- {dir_name}")

dirs, files = list_directory(bucket_name, 'users/john')
print("\nFiles in users/john/:")
for file_name in files:
    print(f"- {file_name}")
```

#### Recursive Directory Download

```python
import opens3
import os

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
bucket_name = 'website-content'
prefix = 'assets/'
local_dir = 'downloaded-assets'

# Create bucket and test data
s3.create_bucket(Bucket=bucket_name)
s3.put_object(Bucket=bucket_name, Key='assets/css/style.css', Body=b'/* CSS content */')
s3.put_object(Bucket=bucket_name, Key='assets/js/script.js', Body=b'// JS content')
s3.put_object(Bucket=bucket_name, Key='assets/images/logo.png', Body=b'PNG CONTENT')

# Function to download a prefix recursively
def download_directory(bucket, prefix, local_dir):
    """Download all objects with a given prefix to a local directory"""
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    for obj in response.get('Contents', []):
        # Get the relative path
        key = obj['Key']
        if key.endswith('/'):
            continue  # Skip directory markers
            
        # Remove prefix from key to get relative path
        relative_path = key[len(prefix):] if key.startswith(prefix) else key
        local_path = os.path.join(local_dir, relative_path)
        
        # Create directories if they don't exist
        local_dir_path = os.path.dirname(local_path)
        if not os.path.exists(local_dir_path):
            os.makedirs(local_dir_path)
            
        # Download the file
        print(f"Downloading {key} to {local_path}")
        s3.download_file(bucket, key, local_path)

# Download the assets directory
download_directory(bucket_name, prefix, local_dir)
print(f"All files downloaded to {local_dir}")
```

### Batch Operations

#### Process All Objects in a Bucket

```python
import opens3
import time

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
bucket_name = 'log-bucket'

# Setup test data
s3.create_bucket(Bucket=bucket_name)
s3.put_object(Bucket=bucket_name, Key='log1.txt', Body=b'line1\nline2\nERROR: Something went wrong\nline4')
s3.put_object(Bucket=bucket_name, Key='log2.txt', Body=b'line1\nline2\nline3\nline4')
s3.put_object(Bucket=bucket_name, Key='log3.txt', Body=b'line1\nERROR: Another error\nline3\nline4')

def process_all_objects(bucket, processor_func):
    """Apply a processor function to all objects in a bucket"""
    response = s3.list_objects_v2(Bucket=bucket)
    
    results = {}
    for obj in response.get('Contents', []):
        key = obj['Key']
        obj_response = s3.get_object(Bucket=bucket, Key=key)
        content = obj_response['Body'].content
        
        # Process the content
        result = processor_func(key, content)
        results[key] = result
    
    return results

# Define a processor function
def find_errors(key, content):
    """Count error lines in content"""
    text = content.decode('utf-8')
    lines = text.split('\n')
    error_lines = [line for line in lines if 'ERROR:' in line]
    return {
        'total_lines': len(lines),
        'error_count': len(error_lines),
        'has_errors': len(error_lines) > 0,
        'error_lines': error_lines
    }

# Process all logs
results = process_all_objects(bucket_name, find_errors)

# Print results
print(f"Error analysis for bucket: {bucket_name}")
print("------------------------------------")
for key, result in results.items():
    print(f"{key}: {result['error_count']} errors out of {result['total_lines']} lines")
    if result['has_errors']:
        for i, line in enumerate(result['error_lines']):
            print(f"  Error {i+1}: {line}")
```

#### Batch Upload from Directory

```python
import opens3
import os
import mimetypes

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
bucket_name = 'uploads'
local_directory = 'files-to-upload'  # Replace with your directory path
s3_prefix = 'batch-upload/'

# Create bucket
s3.create_bucket(Bucket=bucket_name)

# Create some test files if they don't exist
if not os.path.exists(local_directory):
    os.makedirs(local_directory)
    # Create some test files
    with open(os.path.join(local_directory, 'file1.txt'), 'w') as f:
        f.write("Content of file 1")
    with open(os.path.join(local_directory, 'file2.txt'), 'w') as f:
        f.write("Content of file 2")
    os.makedirs(os.path.join(local_directory, 'subfolder'))
    with open(os.path.join(local_directory, 'subfolder', 'file3.txt'), 'w') as f:
        f.write("Content of file 3 in subfolder")

def upload_directory(bucket, local_dir, prefix=''):
    """Upload a directory to S3 recursively"""
    uploaded_files = []
    
    for root, dirs, files in os.walk(local_dir):
        for filename in files:
            # Construct the full local path
            local_path = os.path.join(root, filename)
            
            # Determine the relative path in the S3 bucket
            relative_path = os.path.relpath(local_path, local_dir)
            s3_path = os.path.join(prefix, relative_path).replace('\\', '/')
            
            print(f"Uploading {local_path} to {s3_path}")
            
            # Upload the file
            s3.upload_file(local_path, bucket, s3_path)
            uploaded_files.append(s3_path)
    
    return uploaded_files

# Upload the directory
uploaded = upload_directory(bucket_name, local_directory, s3_prefix)
print(f"Uploaded {len(uploaded)} files:")
for file in uploaded:
    print(f"- {file}")
```

### Error Handling

#### Robust Error Handling

```python
import opens3
from opens3.exceptions import ClientError, NoSuchBucket, NoSuchKey, BucketAlreadyExists
import time

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')

def create_bucket_safely(bucket_name, max_retries=3):
    """Create a bucket with retry logic and error handling"""
    retries = 0
    while retries < max_retries:
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Successfully created bucket: {bucket_name}")
            return True
        except BucketAlreadyExists:
            print(f"Bucket {bucket_name} already exists")
            return False
        except ClientError as e:
            error_code = e.response['Error']['Code'] if hasattr(e, 'response') else 'Unknown'
            error_message = e.response['Error']['Message'] if hasattr(e, 'response') else str(e)
            
            print(f"Error creating bucket (attempt {retries+1}/{max_retries}): {error_code} - {error_message}")
            
            # Handle different error types
            if error_code == '409':  # Conflict
                print(f"Bucket {bucket_name} already exists or name is taken")
                return False
            elif error_code in ['500', '503']:  # Server errors
                # These might be temporary, so we can retry
                retries += 1
                wait_time = 2 ** retries  # Exponential backoff
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                # Other errors might be non-recoverable
                print("Non-recoverable error, aborting")
                return False
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            return False
    
    print(f"Failed to create bucket after {max_retries} attempts")
    return False

def get_object_safely(bucket_name, key):
    """Get an object with proper error handling"""
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        return response['Body'].content
    except NoSuchBucket:
        print(f"Bucket does not exist: {bucket_name}")
        return None
    except NoSuchKey:
        print(f"Object does not exist: {key}")
        return None
    except ClientError as e:
        print(f"Error getting object: {e}")
        return None

# Test our functions
test_bucket = 'error-handling-test'

# Create a bucket
if create_bucket_safely(test_bucket):
    # Upload a test object
    s3.put_object(Bucket=test_bucket, Key='test.txt', Body=b'Test content')
    
    # Try to get objects with error handling
    print("\nTrying to get existing object:")
    content = get_object_safely(test_bucket, 'test.txt')
    if content:
        print(f"Content: {content.decode('utf-8')}")
    
    print("\nTrying to get non-existent object:")
    content = get_object_safely(test_bucket, 'nonexistent.txt')
    if content is None:
        print("Correctly handled missing object")
    
    print("\nTrying to get object from non-existent bucket:")
    content = get_object_safely('nonexistent-bucket', 'test.txt')
    if content is None:
        print("Correctly handled missing bucket")
```

## Advanced Recipes

### Custom Metadata

#### Working with Object Metadata

```python
import opens3
import json

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
bucket_name = 'metadata-test'

# Create a bucket
s3.create_bucket(Bucket=bucket_name)

# Note: OpenS3 may not fully support metadata yet, but this recipe demonstrates
# the approach used with boto3 that would work when the feature is implemented

# Upload an object with metadata
# With boto3, you'd use:
# s3.put_object(
#     Bucket=bucket_name,
#     Key='document.txt',
#     Body=b'Document content',
#     Metadata={
#         'author': 'John Doe',
#         'department': 'Engineering',
#         'created': '2025-05-12'
#     }
# )

# For now, since OpenS3 doesn't support metadata headers directly,
# we can store metadata in a separate object
document_content = b'Document content'
document_key = 'document.txt'
metadata_key = f"{document_key}.metadata.json"

metadata = {
    'author': 'John Doe',
    'department': 'Engineering',
    'created': '2025-05-12'
}

# Upload the document
s3.put_object(Bucket=bucket_name, Key=document_key, Body=document_content)

# Upload the metadata as a separate JSON file
s3.put_object(
    Bucket=bucket_name,
    Key=metadata_key,
    Body=json.dumps(metadata).encode('utf-8')
)

# Retrieve the object and its metadata
document_response = s3.get_object(Bucket=bucket_name, Key=document_key)
document_content = document_response['Body'].content

metadata_response = s3.get_object(Bucket=bucket_name, Key=metadata_key)
metadata_content = metadata_response['Body'].content
metadata = json.loads(metadata_content.decode('utf-8'))

print(f"Document content: {document_content.decode('utf-8')}")
print(f"Document metadata: {metadata}")
```

### Streaming Data

#### Streaming Large Files

```python
import opens3
import io
import time

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
bucket_name = 'streaming-test'

# Create a bucket
s3.create_bucket(Bucket=bucket_name)

# Create a generator that simulates streaming data
def generate_data(chunks=10, chunk_size=1024):
    """Generate chunks of data to simulate streaming"""
    for i in range(chunks):
        # Generate a chunk of data
        chunk = f"Chunk {i}: " + "X" * (chunk_size - len(f"Chunk {i}: "))
        yield chunk.encode('utf-8')
        # Simulate processing time
        time.sleep(0.1)

# Upload streaming data
# Note: For actual streaming in boto3, you'd use StreamingBody
# Here we'll simulate it by creating chunks and uploading them
# as a single object

# Collect all chunks in memory
all_data = b''
for chunk in generate_data(chunks=5):
    print(f"Generated chunk of {len(chunk)} bytes")
    all_data += chunk

# Upload the complete data
s3.put_object(Bucket=bucket_name, Key='stream.txt', Body=all_data)
print(f"Uploaded {len(all_data)} bytes to stream.txt")

# Download and verify
response = s3.get_object(Bucket=bucket_name, Key='stream.txt')
downloaded_data = response['Body'].content
print(f"Downloaded {len(downloaded_data)} bytes")
print(f"Data integrity check: {'PASSED' if downloaded_data == all_data else 'FAILED'}")
```

### Concurrent Operations

#### Parallel Processing with ThreadPoolExecutor

```python
import opens3
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = opens3.client('s3', endpoint_url='http://localhost:8000')
bucket_name = 'concurrent-test'

# Create a bucket and test files
s3.create_bucket(Bucket=bucket_name)

# Create 10 test files
test_files = []
for i in range(10):
    filename = f"testfile_{i}.txt"
    with open(filename, 'w') as f:
        f.write(f"Content for file {i}\n" * 100)  # 100 lines of content
    test_files.append(filename)

def upload_file(file_path):
    """Upload a single file to S3"""
    key = os.path.basename(file_path)
    start_time = time.time()
    s3.upload_file(file_path, bucket_name, key)
    elapsed = time.time() - start_time
    return (key, elapsed)

def download_file(key, output_path):
    """Download a single file from S3"""
    start_time = time.time()
    s3.download_file(bucket_name, key, output_path)
    elapsed = time.time() - start_time
    return (key, elapsed)

# Upload files in parallel
print("Uploading files in parallel...")
with ThreadPoolExecutor(max_workers=5) as executor:
    # Submit all upload tasks
    future_to_file = {executor.submit(upload_file, file): file for file in test_files}
    
    # Process results as they complete
    for future in as_completed(future_to_file):
        file = future_to_file[future]
        try:
            key, elapsed = future.result()
            print(f"Uploaded {file} as {key} in {elapsed:.2f} seconds")
        except Exception as e:
            print(f"Upload of {file} generated an exception: {e}")

# List the uploaded files
response = s3.list_objects_v2(Bucket=bucket_name)
s3_files = [obj['Key'] for obj in response.get('Contents', [])]
print(f"\nFiles in bucket: {s3_files}")

# Create a download directory
download_dir = "downloads"
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

# Download files in parallel
print("\nDownloading files in parallel...")
with ThreadPoolExecutor(max_workers=5) as executor:
    # Submit all download tasks
    future_to_key = {
        executor.submit(download_file, key, os.path.join(download_dir, key)): key 
        for key in s3_files
    }
    
    # Process results as they complete
    for future in as_completed(future_to_key):
        key = future_to_key[future]
        try:
            _, elapsed = future.result()
            print(f"Downloaded {key} in {elapsed:.2f} seconds")
        except Exception as e:
            print(f"Download of {key} generated an exception: {e}")

print(f"\nAll files downloaded to {download_dir}")

# Clean up
print("\nCleaning up...")
for file in test_files:
    os.remove(file)
```

This cookbook provides a comprehensive set of examples for using the OpenS3 SDK in various scenarios. The examples progress from basic to advanced, helping users understand how to effectively work with the SDK for different use cases.
