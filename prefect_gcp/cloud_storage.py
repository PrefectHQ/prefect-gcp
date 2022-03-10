from typing import Union, List, TYPE_CHECKING

from prefect import task
if TYPE_CHECKING:
    from google.cloud.storage import Bucket
    from .credentials import GCPCredentials


@task
async def cloud_storage_get_bucket(
    bucket: str,
    gcp_credentials: "GCPCredentials"
) -> Bucket:
    """
    Retrieve a bucket.

    Args:
        bucket: Name of the bucket.

    Returns:
        The bucket object.
    
    Example:
        Retrieves a bucket named "prefect".
        ```python
        from prefect import flow
        from prefect_gcp import GCPCredentials
        from prefect_gcp.cloud_storage import cloud_storage_get_bucket

        @flow()
        def test_flow():
            gcp_credentials = GCPCredentials("/path/to/service/account/keyfile.json")
            bucket = cloud_storage_get_bucket(gcp_credentials)
            return bucket

        test_flow()
        ```
    """
    client = gcp_credentials.get_client()
    bucket = client.get_bucket()
    return bucket


@task
async def cloud_storage_get_bucket(
    bucket: str,
    gcp_credentials: "GCPCredentials"
) -> Bucket:
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
        def test_flow():
            gcp_credentials = GCPCredentials("/path/to/service/account/keyfile.json")
            bucket = cloud_storage_get_bucket("prefect", gcp_credentials)
            return bucket

        test_flow()
        ```
    """
    client = gcp_credentials.get_client()
    bucket = client.get_bucket(bucket)
    return bucket


@task
async def cloud_storage_create_bucket(
    bucket: str,
    gcp_credentials: "GCPCredentials"
) -> Bucket:
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
        def test_flow():
            gcp_credentials = GCPCredentials("/path/to/service/account/keyfile.json")
            bucket = cloud_storage_create_bucket("prefect", gcp_credentials)
            return bucket

        test_flow()
        ```
    """
    client = gcp_credentials.get_client()
    bucket = client.create_bucket(bucket)
    return bucket
