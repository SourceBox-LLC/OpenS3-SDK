from setuptools import setup, find_packages

setup(
    name="openS3-sdk",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
    ],
    author="SourceBox LLC",
    author_email="your-email@example.com",  # Update with your email
    description="A boto3-like SDK for OpenS3",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/opens3-sdk",  # Update with your repo
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)