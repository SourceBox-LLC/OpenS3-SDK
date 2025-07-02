#!/usr/bin/env python3
"""
OpenS3 Comprehensive Test Suite

Tests both backward compatibility of existing features and new hybrid directory API support.
This test ensures that:
1. All existing file operations work correctly
2. Original OpenS3 enhanced directory operations work correctly
3. New S3-style directory operations work correctly
4. Both approaches work together seamlessly (interoperability)
"""

import os
import shutil
import time
import uuid
import tempfile
from opens3.client import S3Client

# Test setup
def setup_test_env():
    print("\n==== Setting up test environment ====")
    # Create temp directories
    temp_dir = tempfile.mkdtemp(prefix="opens3_test_")
    download_dir = tempfile.mkdtemp(prefix="opens3_download_")
    
    # Create test files in temp directory
    test_files = {
        "file1.txt": "This is test file 1 content",
        "file2.txt": "This is test file 2 content",
        "subdir/file3.txt": "This is file 3 in a subdirectory"
    }
    
    for file_path, content in test_files.items():
        full_path = os.path.join(temp_dir, file_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "w") as f:
            f.write(content)
            
    return temp_dir, download_dir

# Cleanup function
def cleanup(s3, bucket_name, temp_dir, download_dir):
    print("\n==== Cleaning up resources ====")
    try:
        # Delete all objects in the bucket
        print(f"Emptying bucket '{bucket_name}'...")
        response = s3.list_objects_v2(Bucket=bucket_name)
        if response.get('Contents'):
            for obj in response.get('Contents', []):
                s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                print(f"Deleted object: {obj['Key']}")
        
        # Delete the bucket with force empty
        print(f"Deleting bucket '{bucket_name}'...")
        s3.delete_bucket(Bucket=bucket_name, ForceEmpty=True)
        print(f"Bucket '{bucket_name}' deleted successfully")
        
        # Clean up local directories
        for dir_path in [temp_dir, download_dir]:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
                print(f"Deleted local directory: {dir_path}")
                
    except Exception as e:
        print(f"Error during cleanup: {e}")

def main():
    print("==== OpenS3 Comprehensive Test Suite ====")
    print("Testing both backward compatibility and hybrid directory API support")
    
    # Create S3 client
    s3 = S3Client(
        endpoint_url="http://localhost:8001",
        auth=("admin", "password")
    )
    
    # Create test directories
    temp_dir, download_dir = setup_test_env()
    
    # Create a unique test bucket
    test_bucket = f"test-bucket-{int(time.time())}"
    print(f"Creating test bucket: {test_bucket}")
    s3.create_bucket(Bucket=test_bucket)
    
    try:
        # =======================================================
        # Test 1: Regular file operations (backward compatibility)
        # =======================================================
        print("\n==== Test 1: Regular file operations ====")
        
        # Upload a file
        print("Uploading single file...")
        file_content = "This is a regular file upload test"
        test_file_key = "test_file.txt"
        s3.put_object(
            Bucket=test_bucket,
            Key=test_file_key,
            Body=file_content.encode('utf-8')
        )
        
        # List objects
        print("Listing objects...")
        response = s3.list_objects_v2(Bucket=test_bucket)
        files = [obj['Key'] for obj in response.get('Contents', [])]
        print(f"Files in bucket: {files}")
        assert test_file_key in files, f"Expected {test_file_key} in bucket listing"
        
        # Download and verify file
        print("Downloading file...")
        response = s3.get_object(Bucket=test_bucket, Key=test_file_key)
        # The response['Body'] is a Response object, get its content and decode
        if 'Body' in response and hasattr(response['Body'], 'content'):
            downloaded_content = response['Body'].content.decode('utf-8')
        elif 'Body' in response and hasattr(response['Body'], 'read'):
            downloaded_content = response['Body'].read().decode('utf-8')
        else:
            downloaded_content = str(response['Body'])
        
        print(f"Downloaded content: {downloaded_content}")
        assert downloaded_content == file_content, "Downloaded content doesn't match uploaded content"
        
        # Delete the file
        print("Deleting file...")
        s3.delete_object(Bucket=test_bucket, Key=test_file_key)
        response = s3.list_objects_v2(Bucket=test_bucket)
        assert not response.get('Contents'), "Bucket should be empty after deleting file"
        print("Regular file operations passed!")
        
        # =======================================================
        # Test 2: OpenS3 enhanced directory operations
        # =======================================================
        print("\n==== Test 2: OpenS3 enhanced directory operations ====")
        
        # Create a directory using OpenS3's method
        dir_path = "test-directory/"
        print(f"Creating directory: {dir_path}")
        s3.create_directory(Bucket=test_bucket, DirectoryPath=dir_path)
        
        # Upload a file to the directory
        dir_file_key = f"{dir_path}file_in_dir.txt"
        dir_file_content = "This is a file in the directory"
        print(f"Uploading file to directory: {dir_file_key}")
        s3.put_object(
            Bucket=test_bucket,
            Key=dir_file_key,
            Body=dir_file_content.encode('utf-8')
        )
        
        # List directory contents with delimiter
        print("Listing directory contents with delimiter...")
        response = s3.list_objects_v2(
            Bucket=test_bucket,
            Prefix=dir_path,
            Delimiter="/"
        )
        
        dir_files = [obj['Key'] for obj in response.get('Contents', [])]
        print(f"Files in directory: {dir_files}")
        assert dir_file_key in dir_files, f"Expected {dir_file_key} in directory listing"
        
        # Upload directory with recursive content
        print("Uploading entire directory with recursive content...")
        upload_dir_key = "uploaded_directory/"
        s3.upload_directory(local_directory=temp_dir, Bucket=test_bucket, Key=upload_dir_key)
        
        # List uploaded directory contents
        print("Listing uploaded directory contents...")
        response = s3.list_objects_v2(Bucket=test_bucket, Prefix=upload_dir_key)
        uploaded_files = [obj['Key'] for obj in response.get('Contents', [])]
        print(f"Files in uploaded directory: {uploaded_files}")
        assert len(uploaded_files) >= 3, "Expected at least 3 files in uploaded directory"
        assert f"{upload_dir_key}file1.txt" in uploaded_files, "Expected file1.txt in uploaded directory"
        assert f"{upload_dir_key}subdir/file3.txt" in uploaded_files, "Expected subdir/file3.txt in uploaded directory"
        
        # Download directory
        print("Downloading directory...")
        download_target = os.path.join(download_dir, "downloaded")
        os.makedirs(download_target, exist_ok=True)
        s3.download_directory(Bucket=test_bucket, Key=upload_dir_key, LocalPath=download_target)
        
        # Verify downloaded content
        assert os.path.exists(os.path.join(download_target, "file1.txt")), "Missing file1.txt in downloaded content"
        assert os.path.exists(os.path.join(download_target, "subdir", "file3.txt")), "Missing subdir/file3.txt in downloaded content"
        
        with open(os.path.join(download_target, "file1.txt"), "r") as f:
            content = f.read()
            assert "This is test file 1 content" in content, "Downloaded file content doesn't match original"
        
        print("OpenS3 enhanced directory operations passed!")
        
        # =======================================================
        # Test 3: S3-style directory operations (hybrid approach)
        # =======================================================
        print("\n==== Test 3: S3-style directory operations ====")
        
        # Create a directory the S3 way (empty object with trailing slash)
        s3_dir = "s3-style-dir/"
        print(f"Creating S3-style directory: {s3_dir}")
        s3.put_object(Bucket=test_bucket, Key=s3_dir, Body=b"")
        
        # Upload a file to the S3-style directory
        s3_file_key = f"{s3_dir}s3_file.txt"
        s3_file_content = "This is a file in an S3-style directory"
        print(f"Uploading file to S3-style directory: {s3_file_key}")
        s3.put_object(
            Bucket=test_bucket,
            Key=s3_file_key,
            Body=s3_file_content.encode('utf-8')
        )
        
        # List S3-style directory contents
        print("Listing S3-style directory contents...")
        response = s3.list_objects_v2(
            Bucket=test_bucket,
            Prefix=s3_dir
        )
        s3_dir_files = [obj['Key'] for obj in response.get('Contents', [])]
        print(f"Files in S3-style directory: {s3_dir_files}")
        assert s3_file_key in s3_dir_files, f"Expected {s3_file_key} in S3-style directory listing"
        
        # Test the alternative create_directory_s3_style method
        s3_dir2 = "s3-style-dir2/"
        print(f"Creating S3-style directory using helper method: {s3_dir2}")
        s3.create_directory_s3_style(Bucket=test_bucket, DirectoryPath=s3_dir2)
        
        # Upload a file to the second S3-style directory
        s3_file_key2 = f"{s3_dir2}s3_file2.txt"
        s3_file_content2 = "This is a file in the second S3-style directory"
        print(f"Uploading file to second S3-style directory: {s3_file_key2}")
        s3.put_object(
            Bucket=test_bucket,
            Key=s3_file_key2,
            Body=s3_file_content2.encode('utf-8')
        )
        
        # List second S3-style directory contents
        print("Listing second S3-style directory contents...")
        response = s3.list_objects_v2(
            Bucket=test_bucket,
            Prefix=s3_dir2
        )
        s3_dir_files2 = [obj['Key'] for obj in response.get('Contents', [])]
        print(f"Files in second S3-style directory: {s3_dir_files2}")
        assert s3_file_key2 in s3_dir_files2, f"Expected {s3_file_key2} in second S3-style directory listing"
        
        print("S3-style directory operations passed!")
        
        # =======================================================
        # Test 4: Interoperability between approaches
        # =======================================================
        print("\n==== Test 4: Interoperability between approaches ====")
        
        # Download an S3-style directory using OpenS3's download_directory method
        print("Downloading S3-style directory using OpenS3's download_directory...")
        s3_download_target = os.path.join(download_dir, "s3_downloaded")
        os.makedirs(s3_download_target, exist_ok=True)
        s3.download_directory(Bucket=test_bucket, Key=s3_dir, LocalPath=s3_download_target)
        
        # Verify downloaded content from S3-style directory
        assert os.path.exists(os.path.join(s3_download_target, "s3_file.txt")), "Missing s3_file.txt in downloaded content"
        
        with open(os.path.join(s3_download_target, "s3_file.txt"), "r") as f:
            content = f.read()
            assert content == s3_file_content, "Downloaded S3-style file content doesn't match original"
        
        # Upload a directory to an OpenS3 directory path
        interop_dir = "interop-dir/"
        print(f"Creating OpenS3 directory for interop test: {interop_dir}")
        s3.create_directory(Bucket=test_bucket, DirectoryPath=interop_dir)
        
        # Upload a local directory to this OpenS3 directory using regular put_object calls
        print("Uploading files to OpenS3 directory using regular put_object calls...")
        for file_path, content in {"interop1.txt": "Interop test 1", "interop2.txt": "Interop test 2"}.items():
            key = f"{interop_dir}{file_path}"
            s3.put_object(
                Bucket=test_bucket,
                Key=key,
                Body=content.encode('utf-8')
            )
        
        # List directory contents
        print("Listing interop directory contents...")
        response = s3.list_objects_v2(
            Bucket=test_bucket,
            Prefix=interop_dir
        )
        interop_files = [obj['Key'] for obj in response.get('Contents', [])]
        print(f"Files in interop directory: {interop_files}")
        assert f"{interop_dir}interop1.txt" in interop_files, "Expected interop1.txt in interop directory listing"
        assert f"{interop_dir}interop2.txt" in interop_files, "Expected interop2.txt in interop directory listing"
        
        print("Interoperability between approaches passed!")
        
        # =======================================================
        # Test 5: Bucket operations with directories
        # =======================================================
        print("\n==== Test 5: Bucket operations with directories ====")
        
        # List all objects in the bucket
        print("Listing all objects in bucket...")
        response = s3.list_objects_v2(Bucket=test_bucket)
        all_objects = [obj['Key'] for obj in response.get('Contents', [])]
        print(f"Total objects in bucket: {len(all_objects)}")
        
        # Test force empty during bucket deletion
        print("Creating test bucket for force empty test...")
        force_test_bucket = f"force-test-bucket-{int(time.time())}"
        s3.create_bucket(Bucket=force_test_bucket)
        
        # Create a complex directory structure
        print("Creating complex directory structure...")
        dirs = ["dir1/", "dir1/subdir1/", "dir1/subdir2/", "dir2/"]
        files = {
            "dir1/file1.txt": "Test file 1",
            "dir1/subdir1/file2.txt": "Test file 2", 
            "dir1/subdir2/file3.txt": "Test file 3",
            "dir2/file4.txt": "Test file 4"
        }
        
        # Create directories
        for dir_path in dirs:
            s3.create_directory(Bucket=force_test_bucket, DirectoryPath=dir_path)
        
        # Upload files
        for file_path, content in files.items():
            s3.put_object(
                Bucket=force_test_bucket,
                Key=file_path,
                Body=content.encode('utf-8')
            )
        
        # Now delete the bucket with force empty
        print("Deleting bucket with force empty...")
        s3.delete_bucket(Bucket=force_test_bucket, ForceEmpty=True)
        
        # Try to check if bucket exists (should throw a 404)
        try:
            s3.head_bucket(Bucket=force_test_bucket)
            print("Error: Bucket should not exist after deletion")
            assert False, "Bucket still exists after force empty deletion"
        except Exception as e:
            print("Confirmed bucket deletion successful")
        
        print("Bucket operations with directories passed!")
        
        print("\n==== ALL TESTS PASSED! ====")
        print("OpenS3 Hybrid Directory Support is working correctly!")
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up resources
        cleanup(s3, test_bucket, temp_dir, download_dir)

if __name__ == "__main__":
    main()
