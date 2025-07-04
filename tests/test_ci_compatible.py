#!/usr/bin/env python
# CI-compatible tests that can run without a live OpenS3 server

import os
import tempfile
import unittest
from unittest import mock
import pytest

from opens3.client import S3Client


# Function to determine if running in CI mode
def is_ci_mode():
    ci_val = os.environ.get("OPENS3_CI_MODE", "false").lower()
    return ci_val in ("true", "1", "yes", "y", "on")


# Skip the example_compatibility_test in CI mode
pytest.importorskip("conftest")


class TestS3ClientCIMode(unittest.TestCase):
    """Tests for S3Client that can run in CI without a live server"""

    def setUp(self):
        """Setup for each test"""
        # Check if running in CI mode
        self.is_ci = is_ci_mode()
        
        # Use a mock endpoint in CI
        self.endpoint_url = "http://localhost:8001" if not self.is_ci else "http://mock-server"
        self.auth = ("admin", "password")
        
        # Create client with patched session if in CI
        if self.is_ci:
            # Patch the requests module that gets imported inside S3Client.__init__
            self.patcher = mock.patch('requests.Session')
            self.mock_session = self.patcher.start()
            
            # Setup mock responses
            self.mock_response = mock.Mock()
            self.mock_response.status_code = 200
            
            # For create_bucket, the SDK returns ResponseMetadata and Location
            self.create_bucket_response = mock.Mock()
            self.create_bucket_response.status_code = 201
            self.create_bucket_response.json.return_value = {"name": "test-bucket"}
            
            # For list_buckets, the SDK expects a specific format with buckets list
            self.list_buckets_response = mock.Mock()
            self.list_buckets_response.status_code = 200
            self.list_buckets_response.json.return_value = {
                "buckets": [
                    {"name": "test-bucket-1", "creation_date": "2025-07-01T00:00:00"},
                    {"name": "test-bucket-2", "creation_date": "2025-07-02T00:00:00"}
                ]
            }
            
            # For put_object
            self.put_object_response = mock.Mock()
            self.put_object_response.status_code = 201
            self.put_object_response.json.return_value = {"ETag": "\"fake-etag\""}
            
            # For get_object - special handling for download response
            self.get_object_response = mock.Mock()
            self.get_object_response.status_code = 200
            self.get_object_response.content = b"test content"
            self.get_object_response.headers = {"Content-Type": "text/plain"}
            
            # For delete operations
            self.delete_response = mock.Mock()
            self.delete_response.status_code = 204
            # SDK transforms this to a simple HTTPStatusCode response
            self.delete_response.json.return_value = {}
            
            # Default to generic mock response
            self.mock_session.return_value.request.return_value = self.mock_response
            
            # Create client with mock session
            self.client = S3Client(endpoint_url=self.endpoint_url, auth=self.auth)
        else:
            # Create normal client for local testing
            self.client = S3Client(endpoint_url=self.endpoint_url, auth=self.auth)
    
    def tearDown(self):
        """Teardown after each test"""
        if self.is_ci and self.patcher:
            self.patcher.stop()
    
    def test_create_bucket(self):
        """Test create_bucket method"""
        # Skip this test if not in CI mode and no server is running
        if not self.is_ci:
            try:
                # Quick connection test
                import requests
                requests.get(self.endpoint_url, timeout=1)
            except:
                pytest.skip("OpenS3 server not available for testing")
        
        # Set specific response for this test
        if self.is_ci:
            # Set proper response format for create_bucket
            create_response = mock.Mock()
            create_response.status_code = 201
            create_response.json.return_value = {
                "message": "Bucket test-bucket created successfully",
                "location": "/test-bucket"
            }
            self.mock_session.return_value.request.return_value = create_response

        # In CI mode, this will use the mocked session
        response = self.client.create_bucket(Bucket="test-bucket")
        
        # Verify response
        if self.is_ci:
            self.assertIn("ResponseMetadata", response)
            self.assertEqual(response["ResponseMetadata"]["HTTPStatusCode"], 201)
            # Verify the mock was called with the correct arguments
            self.mock_session.return_value.request.assert_called_once()
            args, kwargs = self.mock_session.return_value.request.call_args
            self.assertEqual(args[0].upper(), "POST")  # Method (case-insensitive check)
            self.assertEqual(args[1], f"{self.endpoint_url}/buckets")  # URL
        else:
            self.assertIn("message", response)
    
    def test_list_buckets(self):
        """Test list_buckets method"""
        # Skip this test if not in CI mode and no server is running
        if not self.is_ci:
            try:
                # Quick connection test
                import requests
                requests.get(self.endpoint_url, timeout=1)
            except:
                pytest.skip("OpenS3 server not available for testing")

        # Setup mock for list_buckets specifically
        if self.is_ci:
            self.mock_session.return_value.request.return_value = self.list_buckets_response
        
        # Call list_buckets
        response = self.client.list_buckets()
        
        # Verify response
        if self.is_ci:
            self.assertEqual(len(response["Buckets"]), 2)
            self.assertEqual(response["Buckets"][0]["Name"], "test-bucket-1")
            
            # Case-insensitive check for the HTTP method
            method, url, *_ = self.mock_session.return_value.request.call_args[0]
            self.assertEqual(method.upper(), "GET")
            self.assertEqual(url, f"{self.endpoint_url}/buckets")
        else:
            self.assertIn("Buckets", response)


    def test_put_object(self):
        """Test put_object method"""
        # Skip if not in CI mode and no server running
        if not self.is_ci:
            try:
                import requests
                requests.get(self.endpoint_url, timeout=1)
            except:
                pytest.skip("OpenS3 server not available for testing")
        
        # Set specific responses for this test
        if self.is_ci:
            # Create bucket response
            bucket_response = mock.Mock()
            bucket_response.status_code = 201
            bucket_response.json.return_value = {
                "message": "Bucket test-bucket created successfully",
                "location": "/test-bucket"
            }
            
            # Put object response
            put_response = mock.Mock()
            put_response.status_code = 201
            put_response.json.return_value = {"ETag": "\"fake-etag\""}
            
            # Configure the mock session to return different responses based on the request
            def side_effect(method, url, **kwargs):
                if method.lower() == "post" and url.endswith("/buckets"):
                    return bucket_response
                elif method.lower() == "post" and "objects" in url:
                    return put_response
                return mock.Mock(status_code=404)
                
            self.mock_session.return_value.request.side_effect = side_effect
        
        # First create the bucket
        bucket = "test-bucket"
        if self.is_ci:
            self.client.create_bucket(Bucket=bucket)
        
        # Then put the object
        key = "test-key.txt"
        body = "test content"
        response = self.client.put_object(Bucket=bucket, Key=key, Body=body)
        
        # Verify response
        if self.is_ci:
            self.assertIn("ResponseMetadata", response)
            self.assertEqual(response["ResponseMetadata"]["HTTPStatusCode"], 201)
            self.assertIn("ETag", response)
            
            # Verify the call
            method, url, *_ = self.mock_session.return_value.request.call_args[0]
            self.assertEqual(method.upper(), "POST")  # SDK uses POST for put_object
            self.assertTrue(url.endswith(f"/buckets/{bucket}/objects"))
    
    def test_get_object(self):
        """Test get_object method"""
        # Skip if not in CI mode and no server running
        if not self.is_ci:
            try:
                import requests
                requests.get(self.endpoint_url, timeout=1)
            except:
                pytest.skip("OpenS3 server not available for testing")
        
        # For get_object, we need to create mocks for bucket creation, object creation, and object retrieval
        if self.is_ci:
            # First create a bucket
            bucket_response = mock.Mock()
            bucket_response.status_code = 201
            bucket_response.json.return_value = {"message": "Bucket test-bucket created successfully"}
            
            # Then create an object
            put_response = mock.Mock()
            put_response.status_code = 201
            put_response.json.return_value = {"ETag": "\"fake-etag\""}
            
            # Finally, get the object
            get_response = mock.Mock()
            get_response.status_code = 200
            get_response.content = b"test content"
            get_response.headers = {"Content-Type": "text/plain"}
            
            # Configure the mock session to return different responses based on the request
            def side_effect(method, url, **kwargs):
                if method.lower() == "post" and url.endswith("/buckets"):
                    return bucket_response
                elif method.lower() == "post" and "objects" in url:
                    return put_response
                elif method.lower() == "get" and "object" in url:
                    return get_response
                return mock.Mock(status_code=404)
                
            self.mock_session.return_value.request.side_effect = side_effect
        
        # Create bucket and object first
        if self.is_ci:
            bucket = "test-bucket"
            key = "test-key.txt"
            self.client.create_bucket(Bucket=bucket)
            self.client.put_object(Bucket=bucket, Key=key, Body="test content")
        
        # Call get_object
        response = self.client.get_object(Bucket=bucket, Key=key)
        
        # Verify response
        if self.is_ci:
            self.assertIn("Body", response)
            self.assertIn("ContentLength", response)
            self.assertEqual(response["ContentLength"], len(b"test content"))
            self.assertIn("ContentType", response)
            self.assertEqual(response["ContentType"], "text/plain")
            
            # Verify the call
            method, url, *_ = self.mock_session.return_value.request.call_args[0]
            self.assertEqual(method.upper(), "GET")
    
    def test_delete_object(self):
        """Test delete_object method"""
        # Skip if not in CI mode and no server running
        if not self.is_ci:
            try:
                import requests
                requests.get(self.endpoint_url, timeout=1)
            except:
                pytest.skip("OpenS3 server not available for testing")
        
        # Set specific responses for this test
        if self.is_ci:
            # First create a bucket to avoid 404 errors
            bucket_response = mock.Mock()
            bucket_response.status_code = 201
            bucket_response.json.return_value = {"message": "Bucket test-bucket created successfully"}
            
            # Then set up the delete response
            delete_response = mock.Mock()
            delete_response.status_code = 200
            delete_response.json.return_value = {
                "message": "Object 'test-key.txt' deleted successfully from bucket 'test-bucket'",
                "bucket": "test-bucket",
                "key": "test-key.txt"
            }
            
            # Configure the mock session to return different responses based on the request
            def side_effect(method, url, **kwargs):
                if method.lower() == "post" and url.endswith("/buckets"):
                    return bucket_response
                elif method.lower() == "delete" and "objects" in url:
                    return delete_response
                return mock.Mock(status_code=404)
                
            self.mock_session.return_value.request.side_effect = side_effect
            
        # Create bucket first
        if self.is_ci:
            self.client.create_bucket(Bucket="test-bucket")
            
        # Call delete_object
        bucket = "test-bucket"
        key = "test-key.txt"
        response = self.client.delete_object(Bucket=bucket, Key=key)
        
        # Verify response
        if self.is_ci:
            self.assertIn("ResponseMetadata", response)
            self.assertIn("HTTPStatusCode", response["ResponseMetadata"])
            
            # Verify the call
            method, url, *_ = self.mock_session.return_value.request.call_args[0]
            self.assertEqual(method.upper(), "DELETE")
    
    def test_delete_bucket(self):
        """Test delete_bucket method"""
        # Skip if not in CI mode and no server running
        if not self.is_ci:
            try:
                import requests
                requests.get(self.endpoint_url, timeout=1)
            except:
                pytest.skip("OpenS3 server not available for testing")
        
        # Set specific response for this test
        if self.is_ci:
            self.mock_session.return_value.request.return_value = self.delete_response
        
        # Call delete_bucket
        bucket = "test-bucket"
        response = self.client.delete_bucket(Bucket=bucket)
        
        # Verify response
        if self.is_ci:
            self.assertIn("ResponseMetadata", response)
            self.assertIn("HTTPStatusCode", response["ResponseMetadata"])
            
            # Verify the call
            method, url, *_ = self.mock_session.return_value.request.call_args[0]
            self.assertEqual(method.upper(), "DELETE")
            self.assertEqual(url, f"{self.endpoint_url}/buckets/{bucket}")

# This allows the tests to be run with pytest or unittest
if __name__ == "__main__":
    unittest.main()
