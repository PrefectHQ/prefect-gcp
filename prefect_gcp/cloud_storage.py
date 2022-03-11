import os
from pathlib import Path
from typing import Optional, Union, List, TYPE_CHECKING

from prefect import task, get_run_logger
if TYPE_CHECKING:
    from google.cloud.storage import Bucket
    from .credentials import GCPCredentials


@task
def cloud_storage_create_bucket(
    bucket: str,
    gcp_credentials: "GCPCredentials"
) -> "Bucket":
    """
    Creates a bucket.

    Args:
        bucket: Name of the bucket.
        gcp_credentials: Credentials to use for authentication with GCP.

    Returns:
        The bucket name.
    
    Example:
        Creates a bucket named "prefect".
        ```python
        from prefect import flow
        from prefect_gcp import GCPCredentials
        from prefect_gcp.cloud_storage import cloud_storage_create_bucket

        @flow()
        def example_cloud_storage_create_bucket_flow():
            gcp_credentials = GCPCredentials("/path/to/service/account/keyfile.json")
            bucket = cloud_storage_create_bucket("prefect", gcp_credentials)
            return bucket

        example_cloud_storage_create_bucket_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Creating %s bucket", bucket)

    client = gcp_credentials.get_cloud_storage_client()
    bucket_obj = client.create_bucket(bucket)
    return bucket_obj


def _get_bucket(
        bucket: str,
        gcp_credentials: "GCPCredentials"
    ) -> "Bucket":
    """
    Helper function to retrieve a bucket.
    """
    client = gcp_credentials.get_cloud_storage_client()
    bucket_obj = client.get_bucket(bucket)
    return bucket_obj


@task
def cloud_storage_get_bucket(
    bucket: str,
    gcp_credentials: "GCPCredentials"
) -> "Bucket":
    """
    Retrieve a bucket.

    Args:
        bucket: Name of the bucket.
        gcp_credentials: Credentials to use for authentication with GCP.

    Returns:
        The bucket object.
    
    Example:
        Retrieves a bucket named "prefect".
        ```python
        from prefect import flow
        from prefect_gcp import GCPCredentials
        from prefect_gcp.cloud_storage import cloud_storage_get_bucket

        @flow()
        def example_cloud_storage_get_bucket_flow():
            gcp_credentials = GCPCredentials("/path/to/service/account/keyfile.json")
            bucket = cloud_storage_get_bucket(gcp_credentials)
            return bucket

        example_cloud_storage_get_bucket_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Getting %s bucket", bucket)

    bucket_obj = _get_bucket(bucket, gcp_credentials)
    return bucket_obj


@task
def cloud_storage_download_blob(
    bucket: str,
    blob: str,
    gcp_credentials: "GCPCredentials",
    path: Optional[Union[str, Path]] = None,
) -> Union[str, bytes]:
    """
    Downloads a blob.

    Args:
        bucket: Name of the bucket.
        blob: Name of the Cloud Storage blob.
        gcp_credentials: Credentials to use for authentication with GCP.
        path: If provided, downloads the contents to the provided file path;
            if the path is a directory, automatically joins the blob name.

    Returns:
        The path to the blob object if a path is provided,
        else a `bytes` representation of the blob object.

    Example:
        Downloads blob from bucket.
        ```python
        from prefect import flow
        from prefect_gcp import GCPCredentials
        from prefect_gcp.cloud_storage import cloud_storage_download_blob

        @flow()
        def example_cloud_storage_download_blob_flow():
            gcp_credentials = GCPCredentials("/path/to/service/account/keyfile.json")
            contents = cloud_storage_download_blob("bucket", "blob", gcp_credentials)
            return contents

        example_cloud_storage_download_blob_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Downloading blob named %s from the %s bucket", blob, bucket)

    bucket_obj = _get_bucket(bucket, gcp_credentials)
    blob_obj = bucket_obj.blob(blob)

    if path is not None:
        if isinstance(path, Path):
            path = str(path)
        if os.path.isdir(path):
            path = os.path.join(path, blob)
        blob.download_to_filename(path)
        return path
    else:
        blob_contents = blob_obj.download_as_bytes()
        return blob_contents


@task
def cloud_storage_upload_blob(
    data: Union[bytes, str, Path],
    bucket: str,
    gcp_credentials: "GCPCredentials",
    blob: Optional[str] = None,
) -> str:
    """
    Uploads a blob.

    Args:
        data: String or bytes representation of data to upload, or the
            path to the data.
        bucket: Name of the bucket.
        gcp_credentials: Credentials to use for authentication with GCP.
        blob: Name of the Cloud Storage blob; must be provided if data is
            not a path.

    Returns:
        The blob name.

    Example:
        Uploads blob to bucket.
        ```
        from prefect import flow
        from prefect_gcp import GCPCredentials
        from prefect_gcp.cloud_storage import cloud_storage_upload_blob

        @flow()
        def example_cloud_storage_upload_blob_flow():
            gcp_credentials = GCPCredentials("/path/to/service/account/keyfile.json")
            blob = cloud_storage_upload_blob("data", "bucket", "blob", gcp_credentials)
            return blob

        example_cloud_storage_upload_blob_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Uploading blob named %s to the %s bucket", blob, bucket)

    bucket_obj = _get_bucket(bucket, gcp_credentials)

    if isinstance(data, Path):
        data = str(data)

    is_file_path = os.path.exists(data) and os.path.isfile(data)
    if is_file_path and blob is None:
        blob = os.path.basename(data)

    blob_obj = bucket_obj.blob(blob)

    if is_file_path:
        blob_obj.upload_from_filename(data)
    elif isinstance(data, bytes):
        blob_obj.upload_from_bytes(data)
    else:
        blob_obj.upload_from_string(data)

    return blob
