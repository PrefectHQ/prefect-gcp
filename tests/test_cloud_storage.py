from io import BytesIO
from pathlib import Path

import pytest
from prefect import flow

from prefect_gcp.cloud_storage import (
    cloud_storage_copy_blob,
    cloud_storage_create_bucket,
    cloud_storage_download_blob_as_bytes,
    cloud_storage_download_blob_to_file,
    cloud_storage_upload_blob_from_file,
    cloud_storage_upload_blob_from_string,
)


def test_cloud_storage_create_bucket(gcp_credentials):
    bucket = "expected"
    location = "US"

    @flow
    def test_flow():
        return cloud_storage_create_bucket(bucket, gcp_credentials, location=location)

    assert test_flow() == "expected"


@pytest.mark.parametrize("path", ["/path/somewhere/", Path("/path/somewhere/")])
def test_cloud_storage_download_blob_to_file(path, gcp_credentials):
    @flow
    def test_flow():
        return cloud_storage_download_blob_to_file(
            "bucket", "blob", path, gcp_credentials
        )

    assert test_flow() == path


def test_cloud_storage_download_blob_as_bytes(gcp_credentials):
    @flow
    def test_flow():
        return cloud_storage_download_blob_as_bytes("bucket", "blob", gcp_credentials)

    assert test_flow() == b"bytes"


@pytest.mark.parametrize(
    "file",
    [
        "./file_path.html",
        BytesIO(b"<div>bytes_data</div>"),
    ],
)
def test_cloud_storage_upload_blob_from_file(file, gcp_credentials):
    blob = "blob"
    content_type = "text/html"

    @flow
    def test_flow():
        return cloud_storage_upload_blob_from_file(
            file, "bucket", blob, gcp_credentials, content_type=content_type
        )

    assert test_flow() == blob


@pytest.mark.parametrize(
    "data",
    [
        "str_data",
        b"bytes_data",
    ],
)
@pytest.mark.parametrize("blob", [None, "blob"])
def test_cloud_storage_upload_blob_from_string(data, blob, gcp_credentials):
    blob = "blob"

    @flow
    def test_flow():
        return cloud_storage_upload_blob_from_string(
            data, "bucket", "blob", gcp_credentials
        )

    assert test_flow() == blob


@pytest.mark.parametrize("dest_blob", [None, "dest_blob"])
def test_cloud_storage_copy_blob(dest_blob, gcp_credentials):
    @flow
    def test_flow():
        return cloud_storage_copy_blob(
            "source_bucket",
            "dest_bucket",
            "source_blob",
            gcp_credentials,
            dest_blob=dest_blob,
        )

    if dest_blob is None:
        assert test_flow() == "source_blob"
    else:
        assert test_flow() == "dest_blob"
