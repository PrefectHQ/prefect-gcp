"""Tasks for interacting with GCP Cloud Storage"""

import os
from functools import partial
from typing import TYPE_CHECKING, Optional, Union

from anyio import to_thread
from prefect import get_run_logger, task

if TYPE_CHECKING:
    from pathlib import Path

    from google.cloud.storage import Bucket

    from .credentials import GcpCredentials


@task
async def cloud_storage_create_bucket(
    bucket: str, gcp_credentials: "GcpCredentials"
) -> "Bucket":
    """
    Creates a bucket.

    Args:
        bucket: Name of the bucket.
        gcp_credentials: Credentials to use for authentication with GCP.

    Returns:
        The Bucket object.

    Example:
        Creates a bucket named "prefect".
        ```python
        from prefect import flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.cloud_storage import cloud_storage_create_bucket

        @flow()
        def example_cloud_storage_create_bucket_flow():
            gcp_credentials = GcpCredentials("/path/to/service/account/keyfile.json")
            bucket = cloud_storage_create_bucket("prefect", gcp_credentials)

        example_cloud_storage_create_bucket_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Creating %s bucket", bucket)

    client = gcp_credentials.get_cloud_storage_client()
    partial_create_bucket = partial(client.create_bucket, bucket)
    bucket_obj = await to_thread.run_sync(partial_create_bucket)
    return bucket_obj


async def _get_bucket(bucket: str, gcp_credentials: "GcpCredentials") -> "Bucket":
    """
    Helper function to retrieve a bucket.
    """
    client = gcp_credentials.get_cloud_storage_client()
    partial_get_bucket = partial(client.get_bucket, bucket)
    bucket_obj = await to_thread.run_sync(partial_get_bucket)
    return bucket_obj


@task
async def cloud_storage_get_bucket(
    bucket: str, gcp_credentials: "GcpCredentials"
) -> "Bucket":
    """
    Retrieve a bucket.

    Args:
        bucket: Name of the bucket.
        gcp_credentials: Credentials to use for authentication with GCP.

    Returns:
        The Bucket object.

    Example:
        Retrieves a bucket named "prefect".
        ```python
        from prefect import flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.cloud_storage import cloud_storage_get_bucket

        @flow()
        def example_cloud_storage_get_bucket_flow():
            gcp_credentials = GcpCredentials("/path/to/service/account/keyfile.json")
            bucket = cloud_storage_get_bucket(gcp_credentials)
            return bucket

        example_cloud_storage_get_bucket_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Getting %s bucket", bucket)

    bucket_obj = await _get_bucket(bucket, gcp_credentials)
    return bucket_obj


@task
async def cloud_storage_download_blob(
    bucket: str,
    blob: str,
    gcp_credentials: "GcpCredentials",
    path: Optional[Union[str, "Path"]] = None,
) -> Union[str, "Path", bytes]:
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
        from prefect_gcp import GcpCredentials
        from prefect_gcp.cloud_storage import cloud_storage_download_blob

        @flow()
        def example_cloud_storage_download_blob_flow():
            gcp_credentials = GcpCredentials("/path/to/service/account/keyfile.json")
            contents = cloud_storage_download_blob("bucket", "blob", gcp_credentials)
            return contents

        example_cloud_storage_download_blob_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Downloading blob named %s from the %s bucket", blob, bucket)

    bucket_obj = await _get_bucket(bucket, gcp_credentials)
    partial_blob = partial(bucket_obj.blob, blob)
    blob_obj = await to_thread.run_sync(partial_blob)

    if path is not None:
        if os.path.isdir(path):
            path = os.path.join(path, blob)
        partial_download = partial(blob_obj.download_to_filename, path)
        await to_thread.run_sync(partial_download)
        return path
    else:
        partial_download = partial(blob_obj.download_as_bytes)
        contents = await to_thread.run_sync(partial_download)
        return contents


@task
async def cloud_storage_upload_blob(
    data: Union[bytes, str, "Path"],
    bucket: str,
    gcp_credentials: "GcpCredentials",
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
        from prefect_gcp import GcpCredentials
        from prefect_gcp.cloud_storage import cloud_storage_upload_blob

        @flow()
        def example_cloud_storage_upload_blob_flow():
            gcp_credentials = GcpCredentials("/path/to/service/account/keyfile.json")
            blob = cloud_storage_upload_blob("data", "bucket", "blob", gcp_credentials)
            return blob

        example_cloud_storage_upload_blob_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Uploading blob named %s to the %s bucket", blob, bucket)

    bucket_obj = await _get_bucket(bucket, gcp_credentials)

    is_file_path = os.path.exists(data) and os.path.isfile(data)
    if is_file_path and blob is None:
        blob = os.path.basename(data)
    elif blob is None:
        raise ValueError("Since data is not a path, blob must be provided")

    partial_blob = partial(bucket_obj.blob, blob)
    blob_obj = await to_thread.run_sync(partial_blob)

    if is_file_path:
        partial_upload = partial(blob_obj.upload_from_filename, data)
    elif isinstance(data, bytes):
        partial_upload = partial(blob_obj.upload_from_bytes, data)
    else:
        partial_upload = partial(blob_obj.upload_from_string, data)
    await to_thread.run_sync(partial_upload)

    return blob
