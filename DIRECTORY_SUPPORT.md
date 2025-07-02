# OpenS3 Directory Support

## Hybrid Approach to Directory Management

OpenS3 provides a hybrid approach to directory management, combining the best of both worlds:

1. **S3-Compatible Mode**: Works exactly like AWS S3/boto3, for seamless migration
2. **Enhanced Mode**: More intuitive file-system like operations for directories

This document explains both approaches and how they work together in OpenS3.

## S3-Compatible Directory Operations (AWS S3 Style)

In AWS S3, directories don't technically exist since S3 uses a flat namespace with object keys. 
Directories are simulated using:

1. **Object keys with prefixes**: Objects with common prefixes appear as if they're in directories
2. **Directory markers**: Empty objects with keys ending in `/` to represent directories

```python
# Create a directory the S3/boto3 way
s3.put_object(
    Bucket="my-bucket",
    Key="some/path/to/directory/",  # Note the trailing slash
    Body=b""  # Empty content
)

# Add a file to this "directory"
s3.put_object(
    Bucket="my-bucket",
    Key="some/path/to/directory/file.txt",
    Body=b"File contents"
)

# List objects with prefix to simulate directory listing
response = s3.list_objects_v2(
    Bucket="my-bucket",
    Prefix="some/path/to/directory/"
)
```

OpenS3 fully supports this approach for maximum compatibility with existing S3 code.

## Enhanced Directory Operations (OpenS3 Extensions)

OpenS3 extends the S3 API with more intuitive directory operations that work the way you expect:

```python
# Create a directory using OpenS3's dedicated method
s3.create_directory(
    Bucket="my-bucket",
    DirectoryPath="some/path/to/directory/"
)

# Upload an entire local directory and its contents
s3.upload_directory(
    local_directory="./my_local_dir",
    Bucket="my-bucket", 
    Key="target/path/"
)

# Download an entire directory and its contents
s3.download_directory(
    Bucket="my-bucket",
    Key="some/path/",
    LocalPath="./local_destination"
)
```

## Interoperability

The beauty of OpenS3's approach is that both methods work together seamlessly:

- Directories created with `put_object` (S3-style) can be downloaded with `download_directory` (OpenS3)
- Directories created with `create_directory` (OpenS3) can be accessed with S3-style key prefixes
- Objects within directories are treated identically regardless of how the directory was created

This means you can:

1. Start with familiar S3/boto3 code patterns
2. Gradually migrate to the more intuitive OpenS3 methods as you become comfortable
3. Mix and match approaches as needed for different use cases

## Best Practices

1. **For maximum AWS compatibility**: Use the S3-compatible approach
   ```python
   # Create directory
   s3.put_object(Bucket="my-bucket", Key="dir/", Body=b"")
   
   # For migration compatibility, OpenS3 also provides:
   s3.create_directory_s3_style(Bucket="my-bucket", DirectoryPath="dir/")
   ```

2. **For intuitive directory operations**: Use OpenS3 extensions
   ```python
   # Create directory
   s3.create_directory(Bucket="my-bucket", DirectoryPath="dir/")
   
   # Upload/download entire directories
   s3.upload_directory(local_directory="./local_dir", Bucket="my-bucket", Key="remote_dir/")
   ```

3. **For listing with hierarchical structure**: Use delimiter parameter
   ```python
   # List objects with directory-like structure
   response = s3.list_objects_v2(
       Bucket="my-bucket",
       Prefix="some/path/",
       Delimiter="/"  # Treats "/" as directory separator
   )
   
   # Access files (Contents) and directories (CommonPrefixes)
   for file in response.get('Contents', []):
       print(f"File: {file['Key']}")
   
   for directory in response.get('CommonPrefixes', []):
       print(f"Directory: {directory['Prefix']}")
   ```

## Implementation Details

Under the hood, OpenS3:

1. Creates actual directories on the filesystem
2. Uses `.directory` marker files to identify directories
3. Transparently handles both S3-style directory markers (empty objects with trailing slashes) and OpenS3's enhanced directory structures
4. Provides consistent listing behavior with both approaches
