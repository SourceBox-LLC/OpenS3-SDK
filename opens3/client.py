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
        # Identify download requests by path pattern and query parameters
        is_download_request = False
        
        # Old format: /buckets/{bucket}/objects/{key}
        if (method.lower() == 'get' and 
            path.startswith('/buckets/') and 
            '/objects/' in path and 
            # Don't treat the general list objects endpoint as a download
            not path.endswith('/objects')):
            is_download_request = True
        
        # New format: /buckets/{bucket}/object with object_key query parameter
        if (method.lower() == 'get' and 
            path.startswith('/buckets/') and 
            path.endswith('/object') and
            kwargs.get('params', {}).get('object_key')):
            is_download_request = True
            
        if is_download_request:
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
    
    def delete_bucket(self, Bucket, ForceEmpty=False):
        """
        Delete a bucket.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket.
        ForceEmpty : bool, optional
            If True, forcefully empties the bucket before attempting deletion.
            
        Returns
        -------
        dict
            Response metadata.
        """
        if ForceEmpty:
            # Force delete all objects to ensure bucket is empty
            try:
                # List all objects without delimiter to get everything
                response = self.list_objects_v2(Bucket=Bucket)
                
                # Delete all objects
                if response.get('Contents'):
                    for obj in response['Contents']:
                        self.delete_object(Bucket=Bucket, Key=obj['Key'])
                        
                # Wait a moment for consistency
                import time
                time.sleep(0.5)
            except Exception as e:
                print(f"Warning: Error while force-emptying bucket: {e}")
                
        # Now attempt to delete the bucket
        params = {}
        if ForceEmpty:
            params['force'] = True
            
        response = self._make_api_call('delete', f'/buckets/{Bucket}', params=params)
        
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
    
    def list_objects_v2(self, Bucket, Prefix=None, Delimiter=None):
        """
        List objects in a bucket with support for directory-like hierarchies.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket.
        Prefix : str, optional
            Only return objects that start with this prefix.
        Delimiter : str, optional
            Character used to group keys (typically '/').
            When specified, the response will include CommonPrefixes,
            which are keys between the Prefix and the Delimiter.
            
        Returns
        -------
        dict
            A dictionary containing a list of objects and common prefixes.
        """
        params = {}
        if Prefix:
            params['prefix'] = Prefix
        if Delimiter:
            params['delimiter'] = Delimiter
            
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
    
    def create_directory(self, Bucket, DirectoryPath):
        """Create a directory in a bucket.
        
        This is an OpenS3 extension method for intuitive directory management.
        For S3-compatible code, you can also use put_object with a key ending in '/' and empty content.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket.
        DirectoryPath : str
            The path of the directory to create. Should end with a '/'.
            
        Returns
        -------
        dict
            Response metadata.
        """
        if not DirectoryPath.endswith('/'):
            DirectoryPath = DirectoryPath + '/'
            
        # Use the OpenS3 dedicated directory endpoint
        params = {
            'directory_path': DirectoryPath
        }
        
        response = self._make_api_call('post', f'/buckets/{Bucket}/directories', params=params)
        
        # Convert to boto3-like response
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 201
            }
        }
        
    def create_directory_s3_style(self, Bucket, DirectoryPath):
        """Create a directory in an S3-compatible way (by creating a zero-byte object with trailing slash).
        
        This method provides maximum compatibility with code written for AWS S3/boto3.
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket.
        DirectoryPath : str
            The path of the directory to create. Should end with a '/'.
            
        Returns
        -------
        dict
            Response metadata.
        """
        if not DirectoryPath.endswith('/'):
            DirectoryPath = DirectoryPath + '/'
            
        # Use the standard put_object method with zero-byte content
        return self.put_object(
            Bucket=Bucket,
            Key=DirectoryPath,
            Body=b""
        )
    
    def upload_directory(self, local_directory, Bucket, Key=""):
        """Upload a directory and its contents to a bucket
        
        Parameters
        ----------
        local_directory : str
            The local directory to upload
        Bucket : str
            The name of the bucket
        Key : str
            The key prefix to use for the directory in the bucket
            
        Returns
        -------
        dict
            Dictionary with upload statistics
        """
        import os
        
        if not os.path.isdir(local_directory):
            raise ValueError(f"'{local_directory}' is not a directory")
            
        # Normalize paths
        local_directory = os.path.normpath(local_directory)
        
        # Strip trailing slash from prefix if present
        if Key and Key.endswith('/'):
            Key = Key[:-1]
            
        # Create the root directory marker if needed
        if Key:
            try:
                self.create_directory(Bucket=Bucket, DirectoryPath=Key)
            except Exception as e:
                # If directory already exists, continue
                print(f"Warning: {e}")
        
        stats = {
            'files_uploaded': 0,
            'directories_created': 0,
            'failed_uploads': 0
        }
        
        # Walk directory tree and upload files
        for root, dirs, files in os.walk(local_directory):
            # Calculate relative path from local_directory
            rel_path = os.path.relpath(root, local_directory)
            if rel_path == '.':
                rel_path = ''
                
            # Create subdirectories in bucket
            for dir_name in dirs:
                dir_path = os.path.join(rel_path, dir_name).replace('\\', '/')
                if Key:
                    dir_path = f"{Key}/{dir_path}"
                
                try:
                    self.create_directory(Bucket=Bucket, DirectoryPath=dir_path)
                    stats['directories_created'] += 1
                except Exception as e:
                    # If directory already exists, continue
                    print(f"Warning: Failed to create directory '{dir_path}': {e}")
            
            # Upload files in current directory
            for file_name in files:
                local_file_path = os.path.join(root, file_name)
                s3_key = os.path.join(rel_path, file_name).replace('\\', '/')
                
                if Key:
                    s3_key = f"{Key}/{s3_key}"
                
                # Ensure parent directory exists on server before uploading file
                parent_dir = os.path.dirname(s3_key)
                if parent_dir:
                    try:
                        # Create the parent directory first (will be no-op if it already exists)
                        self.create_directory(Bucket=Bucket, DirectoryPath=parent_dir)
                    except Exception as e:
                        print(f"Warning: Failed to create parent directory '{parent_dir}': {e}")
                        # Continue anyway, the upload might still work
                
                try:
                    self.upload_file(local_file_path, Bucket=Bucket, Key=s3_key)
                    stats['files_uploaded'] += 1
                except Exception as e:
                    print(f"Warning: Failed to upload '{local_file_path}' to '{s3_key}': {e}")
                    stats['failed_uploads'] += 1
        
        return stats
    
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
            The object data and metadata, with a 'Body' key containing the object content.
        """
        # Use the new /buckets/{bucket_name}/object endpoint with query parameters
        # This endpoint is specifically for downloading objects and handles slashes correctly
        params = {
            'object_key': Key
        }
        response = self._make_api_call('get', f'/buckets/{Bucket}/object', params=params)
        
        # Add detailed debugging
        print(f"DEBUG - SDK get_object raw response: {response}")
        print(f"DEBUG - SDK get_object response type: {type(response)}")
        
        # Create boto3-compatible response format
        # For binary content from FileResponse, we need to wrap it in a Body-like object
        if isinstance(response, bytes):
            # Direct bytes response from server, wrap it in a boto3-style response
            class StreamingBody:
                def __init__(self, content):
                    self.content = content
                
                def read(self):
                    return self.content
                
                def __str__(self):
                    try:
                        return self.content.decode('utf-8')
                    except UnicodeDecodeError:
                        return str(self.content)
            
            result = {
                'Body': StreamingBody(response),
                'ContentLength': len(response),
                'ResponseMetadata': {
                    'HTTPStatusCode': 200
                }
            }
            print(f"DEBUG - SDK get_object formatted response with Body")
            return result
        
        # If we got something unexpected, try to make it compatible
        if isinstance(response, dict):
            # If it's a dict but doesn't have 'Body', add a placeholder
            if 'Body' not in response:
                class DummyBody:
                    def __init__(self, content=""):
                        # Ensure content is bytes, not str
                        if isinstance(content, str):
                            self.content = content.encode('utf-8')
                        else:
                            self.content = content if content else b''
                    def read(self):
                        return self.content
                    def __str__(self):
                        return str(self.content)
                
                # Create a binary representation of the response for compatibility
                response['Body'] = DummyBody(f"Unexpected response format: {response}".encode('utf-8'))
                print(f"DEBUG - SDK get_object added dummy Body to dict response")
        else:
            # If it's neither bytes nor dict, wrap the whole thing
            class DummyBody:
                def __init__(self, content):
                    # Ensure content is bytes, not str
                    if isinstance(content, str):
                        self.content = content.encode('utf-8')
                    elif content is None:
                        self.content = b''
                    else:
                        self.content = content
                def read(self):
                    return self.content
                def __str__(self):
                    if isinstance(self.content, bytes):
                        try:
                            return self.content.decode('utf-8')
                        except UnicodeDecodeError:
                            return str(self.content)
                    return str(self.content)
            
            response = {
                'Body': DummyBody(response),
                'ResponseMetadata': {
                    'HTTPStatusCode': 200 if response is not None else 404
                }
            }
            print(f"DEBUG - SDK get_object wrapped non-dict response")
        
        return response
    
    def download_file(self, Bucket, Key, Filename):
        """Download a file from a bucket
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket
        Key : str
            The key of the object
        Filename : str
            The local filename to download to
            
        Returns
        -------
        dict
            Response metadata
        """
        # Use get_object which now handles keys with slashes correctly via query parameters
        response = self.get_object(Bucket, Key)
        
        # Add debugging for troubleshooting directory operations
        print(f"DEBUG - SDK download_file for '{Key}' response status: {response.get('status', 'Unknown')}")
        
        # Ensure the destination directory exists
        import os
        os.makedirs(os.path.dirname(Filename) or '.', exist_ok=True)
        
        # Save the file
        with open(Filename, 'wb') as f:
            for chunk in response['Body'].iter_content(chunk_size=8192):
                f.write(chunk)
        
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        }
        
    def download_directory(self, Bucket, Key, local_directory=None, LocalPath=None):
        """Download a directory and its contents from a bucket
        
        Parameters
        ----------
        Bucket : str
            The name of the bucket
        Key : str
            The key prefix of the directory in the bucket
        local_directory : str
            The local directory to download to (alternatively use LocalPath)
        LocalPath : str
            Alias for local_directory (for compatibility with test scripts)
            
        Returns
        -------
        dict
            Dictionary with download statistics
        """
        # Handle parameter alias
        if LocalPath is not None:
            local_directory = LocalPath
        
        if local_directory is None:
            raise ValueError("Either local_directory or LocalPath must be provided")
        import os
        
        # Ensure the directory exists and normalize it
        os.makedirs(local_directory, exist_ok=True)
        local_directory = os.path.normpath(local_directory)
        
        # Ensure key ends with / for directory listings
        directory_prefix = Key
        if directory_prefix and not directory_prefix.endswith('/'):
            directory_prefix += '/'
            
        # List all objects with the given prefix
        response = self.list_objects_v2(Bucket=Bucket, Prefix=directory_prefix, Delimiter='/')
        
        stats = {
            'files_downloaded': 0,
            'directories_created': 0,
            'failed_downloads': 0
        }
        
        # Process directories (CommonPrefixes)
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                sub_dir = prefix['Prefix']
                # Extract relative directory name
                rel_dir = sub_dir
                if directory_prefix:
                    rel_dir = sub_dir[len(directory_prefix):]
                
                # Create local subdirectory
                sub_local_dir = os.path.join(local_directory, rel_dir)
                os.makedirs(sub_local_dir, exist_ok=True)
                stats['directories_created'] += 1
                
                # Recursively download subdirectory
                sub_stats = self.download_directory(Bucket, sub_dir, sub_local_dir)
                stats['files_downloaded'] += sub_stats['files_downloaded']
                stats['directories_created'] += sub_stats['directories_created']
                stats['failed_downloads'] += sub_stats['failed_downloads']
        
        # Process files (Contents)
        if 'Contents' in response:
            for obj in response['Contents']:
                object_key = obj['Key']
                # Skip directory markers
                if object_key.endswith('/'):
                    continue
                    
                # Extract relative file path
                rel_path = object_key
                if directory_prefix:
                    rel_path = object_key[len(directory_prefix):]
                
                # Construct local file path
                local_file_path = os.path.join(local_directory, rel_path)
                
                try:
                    # Ensure parent directory exists
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    # Download file
                    self.download_file(Bucket=Bucket, Key=object_key, Filename=local_file_path)
                    stats['files_downloaded'] += 1
                except Exception as e:
                    print(f"Warning: Failed to download '{object_key}' to '{local_file_path}': {e}")
                    stats['failed_downloads'] += 1
        
        return stats
    
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
        # Use query parameters instead of path parameters for the object key
        # This allows for keys with slashes (directories) to work correctly
        params = {
            'object_key': Key
        }
        response = self._make_api_call('delete', f'/buckets/{Bucket}/objects', params=params)
        
        print(f"DEBUG - SDK delete_object response: {response}")
        
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