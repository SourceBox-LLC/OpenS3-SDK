"""
OpenS3 Session Management.

This module provides the Session class for managing clients and resources.
"""

import requests
from opens3.client import S3Client
from opens3.utils.auth import get_auth_params


class Session:
    """
    A session for managing connections to OpenS3.
    
    A session manages configuration settings and allows you to create
    service clients and resources.
    """
    
    def __init__(self):
        """
        Initialize a new Session object.
        """
        self._session = requests.Session()
    
    def create_client(self, service_name, **kwargs):
        """
        Create a service client by name.
        
        Parameters
        ----------
        service_name : str
            The name of the service to connect to.
        **kwargs
            Additional arguments to configure the client.
            
        Returns
        -------
        client
            A service client instance
        """
        if service_name.lower() == 's3':
            auth_params = get_auth_params(kwargs)
            endpoint_url = kwargs.get('endpoint_url', 'http://localhost:8000')
            
            return S3Client(
                endpoint_url=endpoint_url,
                auth=auth_params,
                session=self._session
            )
        else:
            raise ValueError(f"Service '{service_name}' not supported")