#!/usr/bin/env python3
"""
Debug script for OpenS3 directory listing
"""

import time
from opens3.client import S3Client

def main():
    print("=== OpenS3 Directory Listing Debug ===\n")
    
    # Create S3 client
    s3 = S3Client(
        endpoint_url="http://localhost:8001",
        auth=("admin", "password")
    )
    
    # Create a test bucket
    bucket_name = f"debug-bucket-{int(time.time())}"
    print(f"Creating bucket: {bucket_name}")
    s3.create_bucket(Bucket=bucket_name)
    
    # 1. Create a directory using OpenS3's method
    dir_path = "test-dir/"
    print(f"\n1. Creating directory: {dir_path}")
    s3.create_directory(Bucket=bucket_name, DirectoryPath=dir_path)
    
    # 2. Add a file to this directory
    file_key = f"{dir_path}file.txt"
    print(f"2. Uploading file: {file_key}")
    s3.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body="Test file content".encode('utf-8')
    )
    
    # 3. Try listing with different approaches
    print("\n3. Listing with various approaches:")
    
    print("\nA. List all objects in bucket (no prefix/delimiter):")
    response = s3.list_objects_v2(Bucket=bucket_name)
    print_objects(response)
    
    print("\nB. List with prefix only:")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=dir_path)
    print_objects(response)
    
    print("\nC. List with prefix and delimiter:")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=dir_path, Delimiter="/")
    print_objects(response)
    
    # Cleanup
    print("\nCleaning up...")
    # Delete the file
    s3.delete_object(Bucket=bucket_name, Key=file_key)
    # Delete the bucket with force empty
    s3.delete_bucket(Bucket=bucket_name, ForceEmpty=True)
    print("Done!")

def print_objects(response):
    print(f"Raw response: {response}")
    if 'Contents' in response:
        print("Files:")
        for obj in response.get('Contents', []):
            print(f"  - {obj['Key']} ({obj['Size']} bytes)")
    else:
        print("No files found.")
        
    if 'CommonPrefixes' in response:
        print("Directories:")
        for prefix in response.get('CommonPrefixes', []):
            print(f"  - {prefix['Prefix']}")

if __name__ == "__main__":
    main()
