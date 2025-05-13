"""
OpenS3 Exceptions.

This module provides the exception classes for OpenS3.
"""

class OpenS3Error(Exception):
    """Base class for all OpenS3 errors."""
    pass

class ClientError(OpenS3Error):
    """Raised when a client-side error occurs."""
    
    def __init__(self, error, operation_name):
        self.response = {
            'Error': error,
            'ResponseMetadata': {
                'HTTPStatusCode': error.get('Code', 500)
            }
        }
        self.operation_name = operation_name
        
        message = f"{error.get('Code', 'Unknown')}: {error.get('Message', 'Unknown')}"
        super().__init__(message)

class BucketAlreadyExists(ClientError):
    """Raised when a bucket already exists."""
    
    def __init__(self, bucket_name):
        error = {
            'Code': '409',
            'Message': f"Bucket '{bucket_name}' already exists"
        }
        super().__init__(error, 'CreateBucket')

class NoSuchBucket(ClientError):
    """Raised when a bucket does not exist."""
    
    def __init__(self, bucket_name):
        error = {
            'Code': '404',
            'Message': f"Bucket '{bucket_name}' does not exist"
        }
        super().__init__(error, 'GetBucket')

class NoSuchKey(ClientError):
    """Raised when an object key does not exist."""
    
    def __init__(self, key):
        error = {
            'Code': '404',
            'Message': f"Object '{key}' does not exist"
        }
        super().__init__(error, 'GetObject')