"""
OpenS3 Client Implementation.

This module provides the client classes for interacting with OpenS3 services.
"""

import os
import datetime
import json
import mimetypes
from urllib.parse import urljoin


class S3Client:
    """
    A low-level client for OpenS3's S3-compatible interface.
    
    This client mimics the boto3 S3 client interface for seamless transition
    from AWS S3 to OpenS3.
    """
    
    def __init__(self, endpoint_url, auth, session=None):
        """
        Initialize a new S3Client.
        
        Parameters
        ----------
        endpoint_url : str
            The URL to the OpenS3 service.
        auth : tuple
            A tuple of (username, password) for HTTP Basic Auth.
        session : requests.Session, optional
            A requests session to use. If not provided, a new one will be created.
        """
        self.endpoint_url = endpoint_url.rstrip('/')
        self.auth = auth
        
        if session is None:
            import requests
            self.session = requests.Session()
        else:
            self.session = session
    
    def _make_api_call(self, method, path, **kwargs):
        """
        Make an API call to the OpenS3 service.
        
        Parameters
        ----------
        method : str
            The HTTP method to use.
        path : str
            The path to the resource.
        **kwargs
            Additional arguments to pass to requests.
            
        Returns
        -------
        dict
            The parsed JSON response.
        """
        url = urljoin(self.endpoint_url, path)
        response = self.session.request(method, url, auth=self.auth, **kwargs)
        
        # Instead of raising, handle error responses
        if 400 <= response.status_code < 600:
            error_detail = 'Unknown error'
            try:
                # Try to extract detailed error message from JSON response
                error_json = response.json()
                if 'detail' in error_json:
                    error_detail = error_json['detail']
                elif 'message' in error_json:
                    error_detail = error_json['message']
            except:
                # Fallback if we can't parse JSON
                error_detail = response.text if response.text else response.reason
                
            # Create a requests HTTPError with the detailed message
            from requests.exceptions import HTTPError
            http_error = HTTPError(f"{response.status_code} {response.reason}: {error_detail}", response=response)
            http_error.detail = error_detail  # Add detail as an attribute for easier access
            http_error.status_code = response.status_code
            raise http_error
        
        # For some calls like get_object, we might not want to parse as JSON
        # Only treat it as a file download if the path matches /buckets/{bucket}/objects/{key}
        # where {key} is a specific object key
        if (method.lower() == 'get' and 
            path.startswith('/buckets/') and 
            '/objects/' in path and 
            # Don't treat the general list objects endpoint as a download
            not path.endswith('/objects')):
            # This is a download_object call for a specific object
            return {
                'Body': response,
                'ContentLength': len(response.content),
                'LastModified': datetime.datetime.now(),  # Placeholder
                'ContentType': response.headers.get('Content-Type', '')
            }
        
        try:
            return response.json()
        except ValueError:
            # Not a JSON response
            return {'ResponseMetadata': {'HTTPStatusCode': response.status_code}}
    
    def create_bucket(self, Bucket):
        """
        Create a new bucket.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket to create.
            
        Returns
        -------
        dict
            Response metadata.
        """
        response = self._make_api_call(
            'post',
            '/buckets',
            json={'name': Bucket}
        )
        
        # Convert to boto3-like response
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 201
            },
            'Location': f'/{Bucket}'
        }
    
    def list_buckets(self):
        """
        List all buckets.
        
        Returns
        -------
        dict
            A dictionary containing a list of buckets.
        """
        response = self._make_api_call('get', '/buckets')
        
        # Convert to boto3-like response
        buckets = []
        for bucket in response.get('buckets', []):
            buckets.append({
                'Name': bucket['name'],
                'CreationDate': datetime.datetime.fromisoformat(bucket['creation_date'])
                                if isinstance(bucket['creation_date'], str) 
                                else bucket['creation_date']
            })
        
        return {
            'Buckets': buckets,
            'Owner': {'ID': 'admin'}  # Placeholder
        }
    
    def delete_bucket(self, Bucket):
        """
        Delete a bucket.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket to delete.
            
        Returns
        -------
        dict
            Response metadata.
        """
        response = self._make_api_call('delete', f'/buckets/{Bucket}')
        
        # Convert to boto3-like response
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        }
    
    def put_object(self, Bucket, Key, Body, **kwargs):
        """
        Add an object to a bucket.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket.
        Key : str
            The key (name) of the object.
        Body : bytes or file-like object
            The content of the object.
        **kwargs : dict
            Additional parameters like ContentType, Metadata, etc.
            
        Returns
        -------
        dict
            Response metadata.
        """
        # For direct content upload, we need to create a temporary file
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            if hasattr(Body, 'read'):
                # File-like object
                temp.write(Body.read())
            else:
                # Bytes or string
                if isinstance(Body, str):
                    Body = Body.encode('utf-8')
                temp.write(Body)
            temp_path = temp.name
        
        try:
            # Now upload the temp file
            with open(temp_path, 'rb') as f:
                files = {'file': (Key, f)}
                
                # Handle additional metadata if provided
                json_data = {}
                if 'Metadata' in kwargs:
                    json_data['metadata'] = kwargs['Metadata']
                
                # Only include json parameter if we have metadata
                if json_data:
                    response = self._make_api_call(
                        'post',
                        f'/buckets/{Bucket}/objects',
                        files=files,
                        data={'json': json.dumps(json_data)}
                    )
                else:
                    response = self._make_api_call(
                        'post',
                        f'/buckets/{Bucket}/objects',
                        files=files
                    )
        finally:
            # Clean up
            os.unlink(temp_path)
        
        # Convert to boto3-like response
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 201
            },
            'ETag': '"fake-etag"'  # OpenS3 doesn't provide ETags yet
        }
    
    def upload_file(self, Filename, Bucket, Key):
        """
        Upload a file to a bucket.
        
        Parameters
        ----------
        Filename : str
            The path to the file to upload.
        Bucket : str
            The name of the bucket.
        Key : str
            The key (name) to give the object in the bucket.
            
        Returns
        -------
        dict
            Response metadata.
        """
        with open(Filename, 'rb') as f:
            files = {'file': (Key or os.path.basename(Filename), f)}
            response = self._make_api_call(
                'post',
                f'/buckets/{Bucket}/objects',
                files=files
            )
        
        # Convert to boto3-like response
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 201
            }
        }
    
    def list_objects_v2(self, Bucket, Prefix=None):
        """
        List objects in a bucket.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket.
        Prefix : str, optional
            Only return objects that start with this prefix.
            
        Returns
        -------
        dict
            A dictionary containing a list of objects.
        """
        params = {}
        if Prefix:
            params['prefix'] = Prefix
            
        response = self._make_api_call(
            'get',
            f'/buckets/{Bucket}/objects',
            params=params
        )
        
        # Convert to boto3-like response
        contents = []
        # Print the actual response for debugging
        print(f"DEBUG - SDK received from server: {response}")
        
        for obj in response.get('objects', []):
            contents.append({
                'Key': obj['key'],
                'LastModified': datetime.datetime.fromisoformat(obj['last_modified'])
                                if isinstance(obj['last_modified'], str) 
                                else obj['last_modified'],
                'Size': obj['size'],
                'ETag': '"fake-etag"',  # OpenS3 doesn't provide ETags yet
                'StorageClass': 'STANDARD'  # OpenS3 doesn't have storage classes
            })
        
        # Print the contents list being returned
        print(f"DEBUG - SDK returning contents: {contents}")
        
        return {
            'Contents': contents,
            'Name': Bucket,
            'Prefix': Prefix or '',
            'MaxKeys': 1000,  # Default in boto3
            'KeyCount': len(contents),
            'IsTruncated': False  # OpenS3 doesn't paginate yet
        }
    
    def get_object(self, Bucket, Key):
        """
        Retrieve an object from a bucket.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket.
        Key : str
            The key of the object.
            
        Returns
        -------
        dict
            The object data and metadata.
        """
        response = self._make_api_call('get', f'/buckets/{Bucket}/objects/{Key}')
        
        # The _make_api_call method handles the specific case of get_object
        return response
    
    def download_file(self, Bucket, Key, Filename):
        """
        Download an object from a bucket to a file.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket.
        Key : str
            The key of the object.
        Filename : str
            The path to save the object to.
            
        Returns
        -------
        dict
            Response metadata.
        """
        response = self.get_object(Bucket, Key)
        
        # Save the content to the file
        with open(Filename, 'wb') as f:
            f.write(response['Body'].content)
        
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        }
    
    def delete_object(self, Bucket, Key):
        """
        Delete an object from a bucket.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket.
        Key : str
            The key of the object.
            
        Returns
        -------
        dict
            Response metadata.
        """
        response = self._make_api_call('delete', f'/buckets/{Bucket}/objects/{Key}')
        
        # Convert to boto3-like response
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        }
        
    def head_bucket(self, Bucket):
        """
        Check if a bucket exists and if the caller has permission to access it.
        
        This method is more efficient than listing all buckets when you only need
        to check the existence of a specific bucket.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket to check.
            
        Returns
        -------
        bool
            True if the bucket exists and the caller has permission to access it,
            False if the bucket does not exist.
            
        Raises
        ------
        HTTPError
            If the caller does not have permission to access the bucket (403) or
            for other errors besides 404 (bucket not found).
        """
        url = urljoin(self.endpoint_url, f'/buckets/{Bucket}')
        try:
            response = self.session.head(url, auth=self.auth)
            
            if response.status_code == 200:
                return True
            elif response.status_code == 404:
                return False
            else:
                # Handle other error codes (e.g., 403 Forbidden)
                response.raise_for_status()
                
        except Exception as e:
            # If it's a 404, return False for bucket not found
            if hasattr(e, 'response') and e.response.status_code == 404:
                return False
            # Re-raise other exceptions
            raise
        
    def list_objects(self, Bucket, Prefix=None):
        """
        List objects in a bucket (legacy method).
        
        This is an alias for list_objects_v2 to maintain compatibility with
        code that uses the older S3 API naming.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket.
        Prefix : str, optional
            Only return objects that start with this prefix.
            
        Returns
        -------
        dict
            A dictionary containing a list of objects.
        """
        return self.list_objects_v2(Bucket, Prefix)
        
    def head_object(self, Bucket, Key):
        """
        Retrieve metadata from an object without returning the object itself.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket.
        Key : str
            The key of the object.
            
        Returns
        -------
        dict
            The object metadata.
        """
        # We need to implement head_object for metadata retrieval
        # In this simple implementation, we'll get metadata from the server
        # by making a special call to the same endpoint
        try:
            # First check if the object exists by getting its size
            response = self._make_api_call(
                'head',
                f'/buckets/{Bucket}/objects/{Key}'
            )
            
            # Try to get metadata from sidecar file if it exists
            metadata = {}
            try:
                metadata_response = self._make_api_call(
                    'get',
                    f'/buckets/{Bucket}/objects/{Key}/metadata'
                )
                if 'metadata' in metadata_response:
                    metadata = metadata_response['metadata']
            except Exception:
                # Metadata endpoint might not exist, which is fine
                pass
                
            # Return in boto3-like format
            return {
                'ContentLength': response.get('size', 0),
                'LastModified': datetime.datetime.fromisoformat(response.get('last_modified', datetime.datetime.now().isoformat()))
                                if isinstance(response.get('last_modified', ''), str)
                                else response.get('last_modified', datetime.datetime.now()),
                'ContentType': response.get('content_type', 'application/octet-stream'),
                'Metadata': metadata
            }
        except Exception as e:
            # If head request fails, the object likely doesn't exist
            raise Exception(f"Object does not exist: {e}")