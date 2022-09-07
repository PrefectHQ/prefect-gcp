import os
from io import BytesIO
from pathlib import Path

import pytest
from prefect import flow

from prefect_gcp.cloud_storage import (
    GcsBucket,
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
        "./file_path",
        BytesIO(b"bytes_data"),
    ],
)
def test_cloud_storage_upload_blob_from_file(file, gcp_credentials):
    blob = "blob"

    @flow
    def test_flow():
        return cloud_storage_upload_blob_from_file(
            file, "bucket", blob, gcp_credentials
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


class TestGcsBucket:
    @pytest.fixture(params=["gs://", "", "basepath", "basepath/"])
    def gcs_bucket(self, gcp_credentials, request):
        return GcsBucket(
            bucket="bucket", gcp_credentials=gcp_credentials, basepath=request.param
        )

    def test_resolve_path(self, gcs_bucket):
        actual = gcs_bucket._resolve_path("subpath")
        basepath = gcs_bucket.basepath
        if not basepath.endswith("/"):
            basepath += "/"
        expected = f"{basepath}subpath"
        assert actual == expected

    def test_read_path(self, gcs_bucket):
        assert gcs_bucket.read_path("blob") == b"bytes"

    def test_write_path(self, gcs_bucket):
        basepath = gcs_bucket.basepath
        if not basepath.endswith("/"):
            basepath += "/"
        assert gcs_bucket.write_path("blob", b"bytes_data") == f"{basepath}blob"

    @pytest.mark.parametrize("from_path", [None, "from_path"])
    @pytest.mark.parametrize("local_path", [None, "local_path"])
    def test_get_directory(self, gcs_bucket, from_path, local_path):
        assert (
            gcs_bucket.get_directory(from_path=from_path, local_path=local_path) is None
        )
        if local_path is None:
            local_path = "."
        assert os.path.exists(os.path.join(local_path, os.path.dirname(from_path)))
