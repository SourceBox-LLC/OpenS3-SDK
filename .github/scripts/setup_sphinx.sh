#!/bin/bash

# Create Sphinx docs if not already set up
if [ ! -f conf.py ]; then
  sphinx-quickstart --no-sep -p "OpenS3 SDK" -a "SourceBox LLC" -v "0.1.0" -r "" -l "en" --ext-autodoc --ext-viewcode --ext-todo
  
  # Enhance conf.py for better docs
  sed -i "s/html_theme = 'alabaster'/html_theme = 'sphinx_rtd_theme'/" conf.py
  sed -i "/import os/a import sys\nsys.path.insert(0, os.path.abspath('..'))" conf.py
  
  # Create index.rst
  cat > index.rst << EOF
OpenS3 SDK Documentation
=======================

A Python SDK for interacting with OpenS3, an S3-compatible storage service.

Installation
-----------

.. code-block:: bash

    pip install opens3

Quick Start
----------

.. code-block:: python

    from opens3 import S3Client
    
    # Create a client
    client = S3Client('http://localhost:8000', ('username', 'password'))
    
    # Create a bucket
    client.create_bucket('my-bucket')
    
    # Upload a file
    client.put_object('my-bucket', 'hello.txt', 'Hello, OpenS3!')
    
    # List objects
    objects = client.list_objects_v2('my-bucket')
    print(objects)

API Reference
------------

.. toctree::
   :maxdepth: 2
   
   api
EOF
  
  # Create api.rst
  cat > api.rst << EOF
API Reference
============

S3Client
-------

.. autoclass:: opens3.client.S3Client
   :members:
   :undoc-members:
   :show-inheritance:
EOF
fi

# Build the docs
sphinx-apidoc -o . ..
make html
