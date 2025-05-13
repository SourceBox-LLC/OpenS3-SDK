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
        
        # Raise for HTTP errors (4XX, 5XX)
        response.raise_for_status()
        
        # For some calls like get_object, we might not want to parse as JSON
        if method.lower() == 'get' and path.startswith('/buckets/') and '/' in path[9:]:
            # This is likely a download_object call
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
    
    def put_object(self, Bucket, Key, Body):
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