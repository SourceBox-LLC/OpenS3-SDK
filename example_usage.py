#!/usr/bin/env python3
"""
Example usage of the OpenS3 SDK.

This script demonstrates how to use the OpenS3 SDK to interact with an OpenS3 server.
Make sure the OpenS3 server is running before executing this script.
"""

import os
import opens3
import tempfile

# Create an S3 client
print("Creating S3 client...")
s3 = opens3.client('s3', 
                  endpoint_url='http://localhost:8000',
                  aws_access_key_id='admin',
                  aws_secret_access_key='password')

# Define a test bucket name
test_bucket = 'test-sdk-bucket'

# Create a bucket
print(f"Creating bucket: {test_bucket}")
s3.create_bucket(Bucket=test_bucket)

# List all buckets
print("Listing all buckets:")
response = s3.list_buckets()
for bucket in response['Buckets']:
    print(f"- {bucket['Name']} (created: {bucket['CreationDate']})")

# Upload an object using put_object
print("Uploading object using put_object...")
s3.put_object(
    Bucket=test_bucket,
    Key='hello.txt',
    Body=b'Hello from OpenS3 SDK!'
)

# Create a temporary file for testing upload_file
temp_file_path = None
with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp:
    temp.write('This is a test file for upload_file')
    temp_file_path = temp.name

# Upload the temporary file
print("Uploading file using upload_file...")
s3.upload_file(temp_file_path, test_bucket, 'test-file.txt')

# List objects in the bucket
print(f"Listing objects in bucket: {test_bucket}")
response = s3.list_objects_v2(Bucket=test_bucket)
for obj in response['Contents']:
    print(f"- {obj['Key']} (size: {obj['Size']} bytes, modified: {obj['LastModified']})")

# Download an object
print("Downloading object...")
response = s3.get_object(Bucket=test_bucket, Key='hello.txt')
content = response['Body'].content
print(f"Content of hello.txt: {content.decode('utf-8')}")

# Download a file
downloaded_file = os.path.join(tempfile.gettempdir(), 'downloaded-test-file.txt')
print(f"Downloading file to: {downloaded_file}")
s3.download_file(test_bucket, 'test-file.txt', downloaded_file)
with open(downloaded_file, 'r') as f:
    print(f"Content of downloaded file: {f.read()}")

# Delete objects
print("Deleting objects...")
s3.delete_object(Bucket=test_bucket, Key='hello.txt')
s3.delete_object(Bucket=test_bucket, Key='test-file.txt')

# Delete the bucket
print(f"Deleting bucket: {test_bucket}")
s3.delete_bucket(Bucket=test_bucket)

# Clean up
if temp_file_path and os.path.exists(temp_file_path):
    os.unlink(temp_file_path)
if os.path.exists(downloaded_file):
    os.unlink(downloaded_file)

print("Example completed successfully!")
