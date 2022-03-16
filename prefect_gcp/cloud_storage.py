"""Tasks for interacting with GCP Cloud Storage"""

import os
from functools import partial
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Tuple, Union

from anyio import to_thread
from prefect import get_run_logger, task

if TYPE_CHECKING:
    from google.cloud.storage import Bucket

    from .credentials import GcpCredentials


@task
async def cloud_storage_create_bucket(
    bucket: str,
    gcp_credentials: "GcpCredentials",
    project: Optional[str] = None,
    location: Optional[str] = None,
) -> str:
    """
    Creates a bucket.

    Args:
        bucket: Name of the bucket.
        gcp_credentials: Credentials to use for authentication with GCP.
        project: Name of the project to use; overrides the
            gcp_credentials project if provided.
        location: Location of the bucket.

    Returns:
        The bucket name.

    Example:
        Creates a bucket named "prefect".
        ```python
        from prefect import flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.cloud_storage import cloud_storage_create_bucket

        @flow()
        def example_cloud_storage_create_bucket_flow():
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json")
            bucket = cloud_storage_create_bucket("prefect", gcp_credentials)

        example_cloud_storage_create_bucket_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Creating %s bucket", bucket)

    client = gcp_credentials.get_cloud_storage_client(project=project)
    partial_create_bucket = partial(client.create_bucket, bucket, location=location)
    await to_thread.run_sync(partial_create_bucket)
    return bucket


async def _get_bucket(
    bucket: str,
    gcp_credentials: "GcpCredentials",
    project: Optional[str] = None,
) -> "Bucket":
    """
    Helper function to retrieve a bucket.
    """
    client = gcp_credentials.get_cloud_storage_client(project=project)
    partial_get_bucket = partial(client.get_bucket, bucket)
    bucket_obj = await to_thread.run_sync(partial_get_bucket)
    return bucket_obj


@task
async def cloud_storage_download_blob(
    bucket: str,
    blob: str,
    gcp_credentials: "GcpCredentials",
    path: Optional[Union[str, "Path"]] = None,
    chunk_size: Optional[int] = None,
    encryption_key: Optional[str] = None,
    timeout: Union[float, Tuple[float, float]] = 60,
    project: Optional[str] = None,
) -> Union[str, "Path", bytes]:
    """
    Downloads a blob.

    Args:
        bucket: Name of the bucket.
        blob: Name of the Cloud Storage blob.
        gcp_credentials: Credentials to use for authentication with GCP.
        path: If provided, downloads the contents to the provided file path;
            if the path is a directory, automatically joins the blob name.
        chunk_size (int, optional): The size of a chunk of data whenever
            iterating (in bytes). This must be a multiple of 256 KB
            per the API specification.
        encryption_key: An encryption key.
        timeout: The number of seconds the transport should wait
            for the server response. Can also be passed as a tuple
            (connect_timeout, read_timeout).
        project: Name of the project to use; overrides the
            gcp_credentials project if provided.

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
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json")
            contents = cloud_storage_download_blob("bucket", "blob", gcp_credentials)
            return contents

        example_cloud_storage_download_blob_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Downloading blob named %s from the %s bucket", blob, bucket)

    bucket_obj = await _get_bucket(bucket, gcp_credentials, project=project)
    partial_blob = partial(
        bucket_obj.blob, blob, chunk_size=chunk_size, encryption_key=encryption_key
    )
    blob_obj = await to_thread.run_sync(partial_blob)

    if path is not None:
        if os.path.isdir(path):
            path = os.path.join(path, blob)
        partial_download = partial(blob_obj.download_to_filename, path, timeout=timeout)
        await to_thread.run_sync(partial_download)
        return path
    else:
        if hasattr(blob_obj, "download_as_bytes"):
            download_method = blob_obj.download_as_bytes
        else:
            download_method = blob_obj.download_as_string
        partial_download = partial(download_method, timeout=timeout)
        contents = await to_thread.run_sync(partial_download)
        return contents


@task
async def cloud_storage_upload_blob(
    data: Union[bytes, str, "Path"],
    bucket: str,
    gcp_credentials: "GcpCredentials",
    blob: Optional[str] = None,
    chunk_size: Optional[int] = None,
    encryption_key: Optional[str] = None,
    timeout: Union[float, Tuple[float, float]] = 60,
    project: Optional[str] = None,
) -> str:
    """
    Uploads a blob.

    Args:
        data: String or bytes representation of data to upload, or if
            uploading from file, must provide the file path as a Path object.
        bucket: Name of the bucket.
        gcp_credentials: Credentials to use for authentication with GCP.
        blob: Name of the Cloud Storage blob; must be provided if data is
            not a path.
        chunk_size (int, optional): The size of a chunk of data whenever
            iterating (in bytes). This must be a multiple of 256 KB
            per the API specification.
        encryption_key: An encryption key.
        timeout: The number of seconds the transport should wait
            for the server response. Can also be passed as a tuple
            (connect_timeout, read_timeout).
        project: Name of the project to use; overrides the
            gcp_credentials project if provided.

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
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json")
            blob = cloud_storage_upload_blob("data", "bucket", "blob", gcp_credentials)
            return blob

        example_cloud_storage_upload_blob_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Uploading blob named %s to the %s bucket", blob, bucket)

    bucket_obj = await _get_bucket(bucket, gcp_credentials, project=project)

    if isinstance(data, Path):
        is_file_path = data.exists() and data.is_file()
    else:
        is_file_path = False

    if is_file_path and blob is None:
        blob = data.name
    elif blob is None:
        raise ValueError("Since data is not a path, blob must be provided")

    partial_blob = partial(
        bucket_obj.blob, blob, chunk_size=chunk_size, encryption_key=encryption_key
    )
    blob_obj = await to_thread.run_sync(partial_blob)

    if is_file_path:
        partial_upload = partial(blob_obj.upload_from_filename, data, timeout=timeout)
    elif isinstance(data, BytesIO):
        partial_upload = partial(blob_obj.upload_from_file, data, timeout=timeout)
    elif isinstance(data, bytes):
        partial_upload = partial(blob_obj.upload_from_bytes, data, timeout=timeout)
    else:
        partial_upload = partial(blob_obj.upload_from_string, data, timeout=timeout)
    await to_thread.run_sync(partial_upload)

    return blob


@task
async def cloud_storage_copy_blob(
    source_bucket: str,
    dest_bucket: str,
    source_blob: str,
    gcp_credentials: "GcpCredentials",
    dest_blob: Optional[str] = None,
    timeout: Union[float, Tuple[float, float]] = 60,
    project: Optional[str] = None,
) -> str:
    """
    Copies data from one Google Cloud Storage bucket to another,
    without downloading it locally.

    Args:
        source_bucket: Source bucket name.
        dest_bucket: Destination bucket name.
        source_blob: Source blob name.
        gcp_credentials: Credentials to use for authentication with GCP.
        dest_blob: Destination blob name.
        timeout: The number of seconds the transport should wait
            for the server response. Can also be passed as a tuple
            (connect_timeout, read_timeout).
        project: Name of the project to use; overrides the
            gcp_credentials project if provided.

    Returns:
        Destination blob name.

    Example:
        Copies blob from one bucket to another.
        ```
        from prefect import flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.cloud_storage import cloud_storage_copy_blob

        @flow()
        def example_cloud_storage_copy_blob_flow():
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json")
            blob = cloud_storage_copy_blob(
                "source_bucket",
                "dest_bucket",
                "source_blob",
                gcp_credentials
            )
            return blob

        example_cloud_storage_copy_blob_flow()
        ```
    """
    logger = get_run_logger()
    logger.info(
        "Copying blob named %s from the %s bucket to the %s bucket",
        source_blob,
        source_bucket,
        dest_bucket,
    )

    source_bucket_obj = await _get_bucket(
        source_bucket, gcp_credentials, project=project
    )

    dest_bucket_obj = await _get_bucket(dest_bucket, gcp_credentials, project=project)
    if dest_blob is None:
        dest_blob = source_blob

    source_blob_obj = source_bucket_obj.blob(source_blob)
    partial_copy_blob = partial(
        source_bucket_obj.copy_blob,
        blob=source_blob_obj,
        destination_bucket=dest_bucket_obj,
        new_name=dest_blob,
        timeout=timeout,
    )
    await to_thread.run_sync(partial_copy_blob)

    return dest_blob
