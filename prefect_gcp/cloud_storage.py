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

    from prefect_gcp.credentials import GcpCredentials


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
async def cloud_storage_download_blob_as_bytes(
    bucket: str,
    blob: str,
    gcp_credentials: "GcpCredentials",
    chunk_size: Optional[int] = None,
    encryption_key: Optional[str] = None,
    timeout: Union[float, Tuple[float, float]] = 60,
    project: Optional[str] = None,
) -> bytes:
    """
    Downloads a blob as bytes.

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
        A bytes or string representation of the blob object.

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
    blob_obj = bucket_obj.blob(
        blob, chunk_size=chunk_size, encryption_key=encryption_key
    )

    partial_download = partial(blob_obj.download_as_bytes, timeout=timeout)
    contents = await to_thread.run_sync(partial_download)
    return contents


@task
async def cloud_storage_download_blob_to_file(
    bucket: str,
    blob: str,
    path: Union[str, Path],
    gcp_credentials: "GcpCredentials",
    chunk_size: Optional[int] = None,
    encryption_key: Optional[str] = None,
    timeout: Union[float, Tuple[float, float]] = 60,
    project: Optional[str] = None,
) -> Union[str, Path]:
    """
    Downloads a blob to a file path.

    Args:
        bucket: Name of the bucket.
        blob: Name of the Cloud Storage blob.
        path: Downloads the contents to the provided file path;
            if the path is a directory, automatically joins the blob name.
        gcp_credentials: Credentials to use for authentication with GCP.
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
        The path to the blob object.

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
            path = cloud_storage_download_blob(
                "bucket", "blob", "data_path", gcp_credentials)
            return path

        example_cloud_storage_download_blob_flow()
        ```
    """
    logger = get_run_logger()
    logger.info(
        "Downloading blob named %s from the %s bucket to %s", blob, bucket, path
    )

    bucket_obj = await _get_bucket(bucket, gcp_credentials, project=project)
    blob_obj = bucket_obj.blob(
        blob, chunk_size=chunk_size, encryption_key=encryption_key
    )

    if os.path.isdir(path):
        if isinstance(path, Path):
            path = path.joinpath(blob)  # keep as Path if Path is passed
        else:
            path = os.path.join(path, blob)  # keep as str if a str is passed

    partial_download = partial(blob_obj.download_to_filename, path, timeout=timeout)
    await to_thread.run_sync(partial_download)
    return path


@task
async def cloud_storage_upload_blob_from_string(
    data: Union[str, bytes],
    bucket: str,
    blob: str,
    gcp_credentials: "GcpCredentials",
    content_type: Optional[str] = None,
    chunk_size: Optional[int] = None,
    encryption_key: Optional[str] = None,
    timeout: Union[float, Tuple[float, float]] = 60,
    project: Optional[str] = None,
) -> str:
    """
    Uploads a blob from a string or bytes representation of data.

    Args:
        data: String or bytes representation of data to upload.
        bucket: Name of the bucket.
        blob: Name of the Cloud Storage blob.
        gcp_credentials: Credentials to use for authentication with GCP.
        content_type: Type of content being uploaded.
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
        ```python
        from prefect import flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.cloud_storage import cloud_storage_upload_blob_from_string

        @flow()
        def example_cloud_storage_upload_blob_from_string_flow():
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json")
            blob = cloud_storage_upload_blob_from_string(
                "data", "bucket", "blob", gcp_credentials)
            return blob

        example_cloud_storage_upload_blob_from_string_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Uploading blob named %s to the %s bucket", blob, bucket)

    bucket_obj = await _get_bucket(bucket, gcp_credentials, project=project)
    blob_obj = bucket_obj.blob(
        blob, chunk_size=chunk_size, encryption_key=encryption_key
    )

    partial_upload = partial(
        blob_obj.upload_from_string, data, content_type=content_type, timeout=timeout
    )
    await to_thread.run_sync(partial_upload)
    return blob


@task
async def cloud_storage_upload_blob_from_file(
    file: Union[str, Path, BytesIO],
    bucket: str,
    blob: str,
    gcp_credentials: "GcpCredentials",
    chunk_size: Optional[int] = None,
    encryption_key: Optional[str] = None,
    timeout: Union[float, Tuple[float, float]] = 60,
    project: Optional[str] = None,
) -> str:
    """
    Uploads a blob from file path or file-like object. Usage for passing in
    file-like object is if the data was downloaded from the web;
    can bypass writing to disk and directly upload to Cloud Storage.

    Args:
        file: Path to data or file like object to upload.
        bucket: Name of the bucket.
        blob: Name of the Cloud Storage blob.
        gcp_credentials: Credentials to use for authentication with GCP.
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
        from prefect_gcp.cloud_storage import cloud_storage_upload_blob_from_file

        @flow()
        def example_cloud_storage_upload_blob_from_file_flow():
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json")
            blob = cloud_storage_upload_blob_from_file(
                "/path/somewhere", "bucket", "blob", gcp_credentials)
            return blob

        example_cloud_storage_upload_blob_from_file_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Uploading blob named %s to the %s bucket", blob, bucket)

    bucket_obj = await _get_bucket(bucket, gcp_credentials, project=project)
    blob_obj = bucket_obj.blob(
        blob, chunk_size=chunk_size, encryption_key=encryption_key
    )

    if isinstance(file, BytesIO):
        partial_upload = partial(blob_obj.upload_from_file, file, timeout=timeout)
    else:
        partial_upload = partial(blob_obj.upload_from_filename, file, timeout=timeout)
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
        dest_blob: Destination blob name; if not provided, defaults to source_blob.
        timeout: The number of seconds the transport should wait
            for the server response. Can also be passed as a tuple
            (connect_timeout, read_timeout).
        project: Name of the project to use; overrides the
            gcp_credentials project if provided.

    Returns:
        Destination blob name.

    Example:
        Copies blob from one bucket to another.
        ```python
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
