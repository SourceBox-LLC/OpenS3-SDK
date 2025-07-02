#!/usr/bin/env python
# Example script to verify backward compatibility with directory support

import os
import tempfile
import shutil
import time
from opens3.client import S3Client

# Configuration
ENDPOINT_URL = "http://localhost:8001"
USERNAME = "admin"  # Replace with your credentials
PASSWORD = "password"  # Replace with your credentials

def test_backward_compatibility():
    """Test both directory features and regular file operations to ensure compatibility."""
    print("Starting backward compatibility test for OpenS3 directory support")
    
    # Create S3 client
    s3 = S3Client(
        endpoint_url=ENDPOINT_URL,
        auth=(USERNAME, PASSWORD)
    )
    
    # Initialize paths for cleanup
    temp_file_path = os.path.join(tempfile.gettempdir(), "test_file.txt")
    download_path = os.path.join(tempfile.gettempdir(), "downloaded_test_file.txt")
    temp_dir = os.path.join(tempfile.gettempdir(), "test_dir_structure")
    download_dir = os.path.join(tempfile.gettempdir(), "downloaded_directory")
    
    # Create a unique test bucket
    import time as time_module  # Use a different name to avoid any potential shadowing
    test_bucket = f"test-bucket-{int(time_module.time())}"
    print(f"Creating test bucket: {test_bucket}")
    s3.create_bucket(Bucket=test_bucket)
    
    try:
        # 1. Test regular file operations (backward compatibility)
        print("\n1. TESTING REGULAR FILE OPERATIONS (BACKWARD COMPATIBILITY)")
        
        # Create a temporary file
        temp_file_path = os.path.join(tempfile.gettempdir(), "test_file.txt")
        with open(temp_file_path, "w") as f:
            f.write("This is a test file for OpenS3 compatibility testing")
        
        # Upload the file using standard method
        print("Uploading file using standard method")
        s3.upload_file(temp_file_path, test_bucket, "test_file.txt")
        
        # List objects (standard way)
        print("Listing objects (standard way)")
        response = s3.list_objects_v2(Bucket=test_bucket)
        print(f"Objects in bucket: {[obj['Key'] for obj in response.get('Contents', [])]}")
        
        # Download the file using standard method
        print("Downloading file using standard method")
        download_path = os.path.join(tempfile.gettempdir(), "downloaded_test_file.txt")
        s3.download_file(test_bucket, "test_file.txt", download_path)
        with open(download_path, "r") as f:
            content = f.read()
        print(f"Downloaded file content: {content}")
        
        # 2. Test directory operations (new functionality)
        print("\n2. TESTING DIRECTORY OPERATIONS (NEW FUNCTIONALITY)")
        
        # Create directory
        print("Creating directory")
        s3.create_directory(Bucket=test_bucket, Key="test_directory/")
        
        # Create a temporary directory structure
        temp_dir = os.path.join(tempfile.gettempdir(), "test_dir_structure")
        os.makedirs(os.path.join(temp_dir, "subdir"), exist_ok=True)
        
        with open(os.path.join(temp_dir, "file1.txt"), "w") as f:
            f.write("File 1 content")
        with open(os.path.join(temp_dir, "subdir", "file2.txt"), "w") as f:
            f.write("File 2 content")
        
        # Upload directory
        print("Uploading directory")
        s3.upload_directory(temp_dir, test_bucket, "uploaded_directory")
        
        # List objects with delimiter
        print("Listing objects with delimiter")
        response = s3.list_objects_v2(Bucket=test_bucket, Delimiter="/")
        print(f"Top-level objects: {[obj['Key'] for obj in response.get('Contents', [])]}")
        print(f"Common prefixes: {[prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])]}")
        
        # Download directory
        print("Downloading directory")
        download_dir = os.path.join(tempfile.gettempdir(), "downloaded_directory")
        if os.path.exists(download_dir):
            shutil.rmtree(download_dir)
        s3.download_directory(test_bucket, "uploaded_directory/", download_dir)
        
        # Verify downloaded structure
        print("Verifying downloaded directory structure")
        for root, dirs, files in os.walk(download_dir):
            rel_path = os.path.relpath(root, download_dir)
            if rel_path == ".":
                rel_path = ""
            print(f"Directory: {rel_path}")
            for file in files:
                print(f"  - File: {file}")
        
        # 3. Mix old and new operations to test compatibility
        print("\n3. TESTING MIXED OPERATIONS (COMPATIBILITY)")
        
        # Upload single file to directory using regular method
        print("Uploading single file to directory using regular method")
        s3.upload_file(temp_file_path, test_bucket, "test_directory/inside_file.txt")
        
        # List directory contents
        print("Listing directory contents")
        response = s3.list_objects_v2(Bucket=test_bucket, Prefix="test_directory/")
        print(f"Directory contents: {[obj['Key'] for obj in response.get('Contents', [])]}")
        
        print("\nAll tests completed successfully!")
        
    finally:
        # 4. CLEANUP
        print("\nCleaning up...")
        try:
            # Empty the bucket (delete all objects and subdirectories recursively)
            def empty_bucket(bucket_name):
                print("Using a more thorough bucket emptying approach...")
                
                # First, list all objects without delimiter to get everything, including directory markers
                response = s3.list_objects_v2(Bucket=bucket_name)
                
                if response.get('Contents'):
                    for obj in response['Contents']:
                        print(f"Deleting object: {obj['Key']}")
                        s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                        
                # Double-check that the bucket is empty
                response = s3.list_objects_v2(Bucket=bucket_name)
                if response.get('Contents'):
                    print(f"Warning: Bucket still contains {len(response['Contents'])} objects after first deletion attempt")
                    
                    # Try a second pass with a different approach
                    for obj in response['Contents']:
                        print(f"Second attempt - Deleting object: {obj['Key']}")
                        s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                        
                # Final verification
                response = s3.list_objects_v2(Bucket=bucket_name)
                if response.get('Contents'):
                    print(f"Warning: Failed to empty bucket. Still contains {len(response['Contents'])} objects")
                    for obj in response['Contents']:
                        print(f"  Remaining object: {obj['Key']}")
                else:
                    print("Bucket emptied successfully.")
            
            # Empty the bucket first
            print("Emptying bucket before deletion...")
            empty_bucket(test_bucket)
            
            # Wait a moment for eventual consistency
            import time
            time.sleep(1)
                    
            # Delete bucket with force empty option
            print("Deleting bucket (with ForceEmpty=True)...")
            s3.delete_bucket(Bucket=test_bucket, ForceEmpty=True)
            print("Bucket deleted successfully")
        except Exception as e:
            print(f"Error during cleanup: {e}")
        
        # Delete temporary files
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        if os.path.exists(download_path):
            os.remove(download_path)
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        if os.path.exists(download_dir):
            shutil.rmtree(download_dir)
        
        print("Cleanup complete")

if __name__ == "__main__":
    test_backward_compatibility()
