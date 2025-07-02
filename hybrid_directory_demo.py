#!/usr/bin/env python3
"""
Hybrid Directory Demo for OpenS3
Demonstrates both S3-compatible and OpenS3-enhanced directory operations
"""

import os
import time
import uuid
from opens3.client import S3Client

def main():
    # Create S3 client
    s3 = S3Client(
        endpoint_url="http://localhost:8001",
        auth=("admin", "password")
    )
    
    # Create a unique test bucket
    test_bucket = f"hybrid-demo-bucket-{int(time.time())}"
    print(f"Creating test bucket: {test_bucket}")
    s3.create_bucket(Bucket=test_bucket)
    
    # ================================================================
    # PART 1: S3-COMPATIBLE DIRECTORY OPERATIONS
    # ================================================================
    print("\n1. DEMONSTRATING S3-COMPATIBLE DIRECTORY OPERATIONS")
    
    # Create a directory the S3/boto3 way (empty object with trailing slash)
    s3_dir = "s3-style-directory/"
    print(f"Creating S3-style directory: {s3_dir}")
    s3.put_object(
        Bucket=test_bucket,
        Key=s3_dir,
        Body=b""  # Empty content
    )
    
    # Create a file in the S3-style directory
    test_file_key = f"{s3_dir}test-file.txt"
    print(f"Uploading file to S3-style directory: {test_file_key}")
    s3.put_object(
        Bucket=test_bucket,
        Key=test_file_key,
        Body=b"This is a file in an S3-style directory"
    )
    
    # List objects with the directory as prefix
    print("Listing objects in S3-style directory:")
    response = s3.list_objects_v2(
        Bucket=test_bucket,
        Prefix=s3_dir
    )
    
    for obj in response.get('Contents', []):
        print(f"  - {obj['Key']} ({obj['Size']} bytes)")
    
    # ================================================================
    # PART 2: OPENS3 ENHANCED DIRECTORY OPERATIONS
    # ================================================================
    print("\n2. DEMONSTRATING OPENS3 ENHANCED DIRECTORY OPERATIONS")
    
    # Create a directory using OpenS3's enhanced directory support
    opens3_dir = "opens3-style-directory/"
    print(f"Creating OpenS3-style directory: {opens3_dir}")
    s3.create_directory(
        Bucket=test_bucket,
        DirectoryPath=opens3_dir
    )
    
    # Upload a directory using OpenS3's enhanced directory support
    # First create a temporary local directory with files
    temp_dir = "temp_dir"
    os.makedirs(temp_dir, exist_ok=True)
    
    # Create some files in the temp directory
    with open(os.path.join(temp_dir, "file1.txt"), "w") as f:
        f.write("This is file 1")
    with open(os.path.join(temp_dir, "file2.txt"), "w") as f:
        f.write("This is file 2")
    
    # Create a subdirectory
    sub_dir = os.path.join(temp_dir, "subdir")
    os.makedirs(sub_dir, exist_ok=True)
    with open(os.path.join(sub_dir, "file3.txt"), "w") as f:
        f.write("This is file 3 in a subdirectory")
    
    # Upload the directory
    print(f"Uploading directory to: {opens3_dir}")
    s3.upload_directory(temp_dir, test_bucket, opens3_dir)
    
    # List objects with directory delimiter
    print("Listing objects in OpenS3 directory with delimiter:")
    response = s3.list_objects_v2(
        Bucket=test_bucket,
        Prefix=opens3_dir,
        Delimiter="/"
    )
    
    print("Files:")
    for obj in response.get('Contents', []):
        print(f"  - {obj['Key']} ({obj['Size']} bytes)")
    
    print("Subdirectories:")
    for prefix in response.get('CommonPrefixes', []):
        print(f"  - {prefix['Prefix']}")
    
    # ================================================================
    # PART 3: DEMONSTRATES INTEROPERABILITY
    # ================================================================
    print("\n3. DEMONSTRATING INTEROPERABILITY")
    
    # Create an S3-style directory using OpenS3's alternative method
    interop_dir = "interop-directory/"
    print(f"Creating S3-style directory using OpenS3 method: {interop_dir}")
    s3.create_directory_s3_style(
        Bucket=test_bucket,
        DirectoryPath=interop_dir
    )
    
    # Upload a file to this directory
    test_file_key = f"{interop_dir}interop-file.txt"
    print(f"Uploading file to interop directory: {test_file_key}")
    s3.put_object(
        Bucket=test_bucket,
        Key=test_file_key,
        Body=b"This demonstrates interoperability between approaches"
    )
    
    # Now download the entire directory using OpenS3's enhanced method
    print("Downloading interop directory using OpenS3 enhanced method...")
    download_dir = "downloaded_interop"
    os.makedirs(download_dir, exist_ok=True)
    
    s3.download_directory(
        Bucket=test_bucket,
        Key=interop_dir,
        LocalPath=download_dir
    )
    
    print(f"Files downloaded to {download_dir}:")
    for root, dirs, files in os.walk(download_dir):
        for file in files:
            print(f"  - {os.path.join(root, file)}")
    
    # ================================================================
    # CLEANUP
    # ================================================================
    print("\nCleaning up resources...")
    # Clean up local directories
    for dir_path in [temp_dir, download_dir]:
        if os.path.exists(dir_path):
            for root, dirs, files in os.walk(dir_path, topdown=False):
                for file in files:
                    os.remove(os.path.join(root, file))
                for dir in dirs:
                    os.rmdir(os.path.join(root, dir))
            os.rmdir(dir_path)
    
    # Delete the bucket with force empty
    print(f"Deleting bucket {test_bucket}...")
    s3.delete_bucket(Bucket=test_bucket, ForceEmpty=True)
    print("Demo completed successfully!")

if __name__ == "__main__":
    main()
