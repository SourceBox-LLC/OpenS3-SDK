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
    return os.environ.get("OPENS3_CI_MODE", "false").lower() == "true"


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
            self.mock_response.json.return_value = {"message": "Success"}
            
            # Make session.request return our mock response
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

        # In CI mode, this will use the mocked session
        response = self.client.create_bucket(Bucket="test-bucket")
        
        # Verify response
        if self.is_ci:
            self.assertEqual(response["message"], "Success")
            # Verify the mock was called with the correct arguments
            self.mock_session.return_value.request.assert_called_once()
            args, kwargs = self.mock_session.return_value.request.call_args
            self.assertEqual(args[0], "POST")  # Method
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
            mock_buckets_response = mock.Mock()
            mock_buckets_response.status_code = 200
            mock_buckets_response.json.return_value = {
                "buckets": [
                    {"name": "test-bucket-1", "creation_date": "2025-07-01T00:00:00Z"},
                    {"name": "test-bucket-2", "creation_date": "2025-07-02T00:00:00Z"}
                ]
            }
            self.mock_session.return_value.request.return_value = mock_buckets_response
        
        # Call list_buckets
        response = self.client.list_buckets()
        
        # Verify response
        if self.is_ci:
            self.assertEqual(len(response["Buckets"]), 2)
            self.assertEqual(response["Buckets"][0]["Name"], "test-bucket-1")
            self.mock_session.return_value.request.assert_called_with(
                "GET", f"{self.endpoint_url}/buckets", auth=self.auth
            )
        else:
            self.assertIn("Buckets", response)


# This allows the tests to be run with pytest or unittest
if __name__ == "__main__":
    unittest.main()
