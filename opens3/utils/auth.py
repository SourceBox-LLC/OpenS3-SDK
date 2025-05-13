"""
Authentication utilities for OpenS3.

This module provides authentication-related functions for the OpenS3 SDK.
"""

def get_auth_params(kwargs):
    """
    Extract auth parameters from kwargs with support for boto3-style parameters.
    
    Parameters
    ----------
    kwargs : dict
        Keyword arguments that may contain auth information.
        
    Returns
    -------
    tuple
        A tuple of (username, password) for OpenS3 authentication.
    """
    # Direct auth parameter (like requests)
    if 'auth' in kwargs:
        return kwargs['auth']
    
    # Boto3-style parameters
    aws_access_key_id = kwargs.get('aws_access_key_id')
    aws_secret_access_key = kwargs.get('aws_secret_access_key')
    
    if aws_access_key_id and aws_secret_access_key:
        return (aws_access_key_id, aws_secret_access_key)
    
    # Default to admin/password (matching the OpenS3 defaults)
    return ('admin', 'password')