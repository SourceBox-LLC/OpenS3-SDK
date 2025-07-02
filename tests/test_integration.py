#!/usr/bin/env python
# Integration tests for OpenS3 SDK with a real OpenS3 server

import os
import unittest
import tempfile
import shutil
import time
import uuid

from opens3.client import S3Client


def is_integration_test():
    """Check if we're running integration tests with a live server"""
    return os.environ.get("OPENS3_INTEGRATION_TEST", "").lower() in ("1", "true", "yes", "y")


# Skip this test suite if not in integration test mode
def skip_if_no_server():
    if not is_integration_test():
        return unittest.skip("Integration test environment not detected")
    return lambda func: func


@skip_if_no_server()
class TestS3ClientIntegration(unittest.TestCase):
    """Integration tests for S3Client with a live OpenS3 server"""

    def setUp(self):
        """Setup for each test"""
        # Get server config from environment variables
        self.endpoint_url = os.environ.get("OPENS3_ENDPOINT", "http://localhost:8001")
        self.auth = (
            os.environ.get("OPENS3_AUTH_USER", "admin"),
            os.environ.get("OPENS3_AUTH_PASS", "password")
        )
        
        # Create client
        self.client = S3Client(endpoint_url=self.endpoint_url, auth=self.auth)
        
        # Create a unique test bucket for this test run
        self.test_bucket = f"test-bucket-{uuid.uuid4().hex[:8]}"
        self.client.create_bucket(Bucket=self.test_bucket)

        # Test data
        self.test_content = b"This is test content for integration testing."
        self.test_key = "test-file.txt"
        
        # Create temp directory for file operations
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Teardown after each test"""
        # Clean up by deleting all objects in the bucket
        try:
            response = self.client.list_objects_v2(Bucket=self.test_bucket)
            if 'Contents' in response:
                for obj in response['Contents']:
                    self.client.delete_object(Bucket=self.test_bucket, Key=obj['Key'])
            
            # Delete the bucket
            self.client.delete_bucket(Bucket=self.test_bucket, ForceEmpty=True)
        except Exception as e:
            print(f"Error during cleanup: {e}")
        
        # Remove temp directory
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_bucket_operations(self):
        """Test basic bucket operations"""
        # List buckets
        response = self.client.list_buckets()
        self.assertIn("Buckets", response)
        
        # Find our test bucket
        bucket_found = False
        for bucket in response["Buckets"]:
            if bucket["Name"] == self.test_bucket:
                bucket_found = True
                break
        
        self.assertTrue(bucket_found, f"Test bucket {self.test_bucket} not found in bucket list")
    
    def test_object_operations(self):
        """Test basic object operations"""
        # Upload object
        self.client.put_object(
            Bucket=self.test_bucket,
            Key=self.test_key,
            Body=self.test_content
        )
        
        # List objects
        response = self.client.list_objects_v2(Bucket=self.test_bucket)
        self.assertIn("Contents", response)
        
        # Find our test object
        object_found = False
        for obj in response["Contents"]:
            if obj["Key"] == self.test_key:
                object_found = True
                break
        
        self.assertTrue(object_found, f"Test object {self.test_key} not found in object list")
        
        # Get object
        response = self.client.get_object(
            Bucket=self.test_bucket,
            Key=self.test_key
        )
        
        self.assertIn("Body", response)
        # The Body field contains a Response object, we need to extract the content
        if hasattr(response["Body"], "content"):
            # If it's a Response object with content attribute
            self.assertEqual(response["Body"].content, self.test_content)
        elif hasattr(response["Body"], "read"):
            # If it's a StreamingBody-like object
            self.assertEqual(response["Body"].read(), self.test_content)
        else:
            # Fallback for direct content
            self.assertEqual(response["Body"], self.test_content)
        
        # Delete object
        self.client.delete_object(
            Bucket=self.test_bucket,
            Key=self.test_key
        )
        
        # Verify deletion
        response = self.client.list_objects_v2(Bucket=self.test_bucket)
        if "Contents" in response:
            for obj in response["Contents"]:
                self.assertNotEqual(obj["Key"], self.test_key)
    
    def test_file_operations(self):
        """Test file upload/download operations"""
        # Create test file
        local_file = os.path.join(self.temp_dir, "upload.txt")
        with open(local_file, "wb") as f:
            f.write(self.test_content)
        
        # Upload file
        self.client.upload_file(
            local_file,
            self.test_bucket,
            "uploaded-file.txt"
        )
        
        # Download file
        download_path = os.path.join(self.temp_dir, "download.txt")
        self.client.download_file(
            self.test_bucket,
            "uploaded-file.txt",
            download_path
        )
        
        # Verify content
        with open(download_path, "rb") as f:
            content = f.read()
        
        self.assertEqual(content, self.test_content)
    
    def test_directory_operations(self):
        """Test directory operations"""
        # Create directory structure
        dir_path = os.path.join(self.temp_dir, "test_dir")
        subdir_path = os.path.join(dir_path, "subdir")
        os.makedirs(subdir_path, exist_ok=True)
        
        # Create test files
        with open(os.path.join(dir_path, "file1.txt"), "wb") as f:
            f.write(b"File 1 content")
        
        with open(os.path.join(subdir_path, "file2.txt"), "wb") as f:
            f.write(b"File 2 content")
        
        # Create test directory in bucket
        self.client.create_directory(
            Bucket=self.test_bucket,
            directory="test_directory"
        )
        
        # Upload directory
        self.client.upload_directory(
            dir_path,
            self.test_bucket,
            "uploaded_directory"
        )
        
        # List objects to verify upload
        response = self.client.list_objects_v2(
            Bucket=self.test_bucket,
            Prefix="uploaded_directory/"
        )
        
        self.assertIn("Contents", response)
        self.assertTrue(len(response["Contents"]) >= 2)
        
        # Download directory
        download_dir = os.path.join(self.temp_dir, "downloaded")
        os.makedirs(download_dir, exist_ok=True)
        
        self.client.download_directory(
            self.test_bucket,
            "uploaded_directory",
            download_dir
        )
        
        # Verify downloaded files
        self.assertTrue(os.path.exists(os.path.join(download_dir, "file1.txt")))
        self.assertTrue(os.path.exists(os.path.join(download_dir, "subdir", "file2.txt")))


# This allows the tests to be run with pytest
if __name__ == "__main__":
    unittest.main()
