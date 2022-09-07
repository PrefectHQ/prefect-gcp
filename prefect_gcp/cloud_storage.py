"""Tasks for interacting with GCP Cloud Storage"""

import os
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Tuple, Union
from uuid import uuid4

from prefect import get_run_logger, task
from prefect.filesystems import ReadableFileSystem, WritableFileSystem
from prefect.logging import disable_run_logger
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from prefect.utilities.filesystem import filter_files

# cannot be type_checking only or else `fields = cls.schema()` raises
# TypeError: issubclass() arg 1 must be a class
from prefect_gcp.credentials import GcpCredentials

if TYPE_CHECKING:
    from google.cloud.storage import Bucket


@task
async def cloud_storage_create_bucket(
    bucket: str,
    gcp_credentials: GcpCredentials,
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
    await run_sync_in_worker_thread(client.create_bucket, bucket, location=location)
    return bucket


async def _get_bucket(
    bucket: str,
    gcp_credentials: GcpCredentials,
    project: Optional[str] = None,
) -> "Bucket":
    """
    Helper function to retrieve a bucket.
    """
    client = gcp_credentials.get_cloud_storage_client(project=project)
    bucket_obj = await run_sync_in_worker_thread(client.get_bucket, bucket)
    return bucket_obj


@task
async def cloud_storage_download_blob_as_bytes(
    bucket: str,
    blob: str,
    gcp_credentials: GcpCredentials,
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

    contents = await run_sync_in_worker_thread(
        blob_obj.download_as_bytes, timeout=timeout
    )
    return contents


@task
async def cloud_storage_download_blob_to_file(
    bucket: str,
    blob: str,
    path: Union[str, Path],
    gcp_credentials: GcpCredentials,
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

    await run_sync_in_worker_thread(
        blob_obj.download_to_filename, path, timeout=timeout
    )
    return path


@task
async def cloud_storage_upload_blob_from_string(
    data: Union[str, bytes],
    bucket: str,
    blob: str,
    gcp_credentials: GcpCredentials,
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

    await run_sync_in_worker_thread(
        blob_obj.upload_from_string, data, content_type=content_type, timeout=timeout
    )
    return blob


@task
async def cloud_storage_upload_blob_from_file(
    file: Union[str, Path, BytesIO],
    bucket: str,
    blob: str,
    gcp_credentials: GcpCredentials,
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
        await run_sync_in_worker_thread(
            blob_obj.upload_from_file, file, timeout=timeout
        )
    else:
        await run_sync_in_worker_thread(
            blob_obj.upload_from_filename, file, timeout=timeout
        )
    return blob


@task
async def cloud_storage_copy_blob(
    source_bucket: str,
    dest_bucket: str,
    source_blob: str,
    gcp_credentials: GcpCredentials,
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
    await run_sync_in_worker_thread(
        source_bucket_obj.copy_blob,
        blob=source_blob_obj,
        destination_bucket=dest_bucket_obj,
        new_name=dest_blob,
        timeout=timeout,
    )

    return dest_blob


class GcsBucket(ReadableFileSystem, WritableFileSystem):
    """
    Block used to store data using GCP Cloud Storage Buckets.

    Attributes:
        bucket: Name of the bucket.
        gcp_credentials: The credentials to authenticate with GCP.
        basepath: Used when you don't want to read/write at base level.

    Example:
        Load stored GCP Cloud Storage Bucket:
        ```python
        from prefect_gcp import GcpCloudStorageBucket
        gcp_cloud_storage_bucket_block = GcpCloudStorageBucket.load("BLOCK_NAME")
        ```
    """

    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/4CD4wwbiIKPkZDt4U3TEuW/c112fe85653da054b6d5334ef662bec4/gcp.png?h=250"  # noqa
    _block_type_name = "GCS Bucket"

    bucket: str
    gcp_credentials: GcpCredentials
    basepath: str = "gs://"

    def _resolve_path(self, path: str) -> str:
        """
        A helper function used in write_path to join `self.basepath` and `path`.

        Args:
            path: Name of the key, e.g. "file1". Each object in your
                bucket has a unique key (or key name).

        Returns:
            The joined path.
        """
        path = path or str(uuid4())

        basepath = (
            self.basepath + "/" if not self.basepath.endswith("/") else self.basepath
        )

        # If basepath provided, it means we won't write to the root dir of
        # the bucket. So we need to add it on the front of the path.
        path = f"{basepath}{path}" if basepath else path
        return path

    @sync_compatible
    async def get_directory(
        self, from_path: Optional[str] = None, local_path: Optional[str] = None
    ) -> None:
        """
        Copies a folder from the configured GCS bucket to a local directory.
        Defaults to copying the entire contents of the block's basepath to the current
        working directory.

        Args:
            from_path: Path in GCS bucket to download from. Defaults to the block's
                configured basepath.
            local_path: Local path to download GCS bucket contents to.
                Defaults to the current working directory.
        """
        if from_path is None:
            from_path = self.basepath

        if local_path is None:
            local_path = str(Path(".").absolute())

        project = self.gcp_credentials.project
        client = self.gcp_credentials.get_cloud_storage_client(project=project)

        for blob in client.list_blobs(self.bucket):
            if blob.name[-1] == "/":
                # object is a folder and will be created if it contains any objects
                continue
            local_file_path = os.path.join(local_path, from_path)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            with disable_run_logger():
                await cloud_storage_download_blob_to_file.fn(
                    bucket=self.bucket,
                    blob=blob,
                    path=local_file_path,
                    gcp_credentials=self.gcp_credentials,
                )

    @sync_compatible
    async def put_directory(
        self,
        local_path: Optional[str] = None,
        to_path: Optional[str] = None,
        ignore_file: Optional[str] = None,
    ) -> int:
        """
        Uploads a directory from a given local path to the configured GCS bucket in a
        given folder.

        Defaults to uploading the entire contents the current working directory to the
        block's basepath.

        Args:
            local_path: Path to local directory to upload from.
            to_path: Path in GCS bucket to upload to. Defaults to block's configured
                basepath.
            ignore_file: Path to file containing gitignore style expressions for
                filepaths to ignore.

        Returns:
            The number of files uploaded.
        """
        if local_path is None:
            local_path = "."

        if to_path is None:
            to_path = self.basepath

        included_files = None
        if ignore_file:
            with open(ignore_file, "r") as f:
                ignore_patterns = f.readlines()
            included_files = filter_files(local_path, ignore_patterns)

        uploaded_file_count = 0
        for local_file_path in Path(local_path).rglob("*"):
            if included_files is not None and local_file_path not in included_files:
                continue
            elif not local_file_path.is_dir():
                remote_file_path = Path(to_path) / local_file_path.name
                with open(local_file_path, "rb") as local_file:
                    local_file_content = local_file.read()
                await self.write_path(str(remote_file_path), content=local_file_content)
                uploaded_file_count += 1

        return uploaded_file_count

    @sync_compatible
    async def read_path(self, path: str) -> bytes:
        """
        Read specified path from GCS and return contents. Provide the entire
        path to the key in GCS.

        Args:
            path: Entire path to (and including) the key.

        Returns:
            A bytes or string representation of the blob object.
        """
        path = self._resolve_path(path)
        with disable_run_logger():
            contents = await cloud_storage_download_blob_as_bytes.fn(
                bucket=self.bucket, blob=path, gcp_credentials=self.gcp_credentials
            )
        return contents

    @sync_compatible
    async def write_path(self, path: str, content: bytes) -> str:
        """
        Writes to an GCS bucket.

        Args:
            path: The key name. Each object in your bucket has a unique
                key (or key name).
            content: What you are uploading to GCS Bucket.

        Returns:
            The path that the contents were written to.
        """
        path = self._resolve_path(path)
        with disable_run_logger():
            await cloud_storage_upload_blob_from_string.fn(
                data=content,
                bucket=self.bucket,
                blob=path,
                gcp_credentials=self.gcp_credentials,
            )
        return path
