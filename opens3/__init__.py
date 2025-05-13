"""
OpenS3 SDK - A boto3-like interface for OpenS3.

This module provides a simple interface for interacting with the OpenS3 service,
mimicking the AWS boto3 library's interface for easy adoption by AWS users.
"""

from opens3.session import Session

__version__ = '0.1.0'


def client(service_name, **kwargs):
    """
    Create a low-level service client by name.

    Parameters
    ----------
    service_name : str
        The name of the service to connect to. Currently only 's3' is supported.
    **kwargs
        Keyword arguments to pass to the Session.create_client method.

    Returns
    -------
    opens3.client.S3Client
        A low-level client instance
    """
    if service_name.lower() != 's3':
        raise ValueError(f"Service '{service_name}' not supported. Only 's3' is currently available.")
    
    session = Session()
    return session.create_client(service_name, **kwargs)


def resource(service_name, **kwargs):
    """
    Create a resource service client by name.

    Parameters
    ----------
    service_name : str
        The name of the service to connect to. Currently only 's3' is supported.
    **kwargs
        Keyword arguments to pass to the Session.create_resource method.

    Returns
    -------
    opens3.resource.S3Resource
        A resource client instance
    """
    if service_name.lower() != 's3':
        raise ValueError(f"Service '{service_name}' not supported. Only 's3' is currently available.")
    
    session = Session()
    # For the initial version, resource is not implemented yet
    raise NotImplementedError("Resource interface is not yet implemented")