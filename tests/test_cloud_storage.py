import os
from io import BytesIO
from pathlib import Path

import pytest
from prefect import flow

from prefect_gcp.cloud_storage import (
    cloud_storage_copy_blob,
    cloud_storage_create_bucket,
    cloud_storage_download_blob,
    cloud_storage_upload_blob,
)


def test_cloud_storage_create_bucket(gcp_credentials):
    bucket = "expected"
    location = "US"

    @flow
    def test_flow():
        return cloud_storage_create_bucket(bucket, gcp_credentials, location=location)

    assert test_flow().result().result() == "expected"


@pytest.mark.parametrize("path", [None, "/path/somewhere/", Path("/path/somewhere/")])
def test_cloud_storage_download_blob(path, gcp_credentials):
    @flow
    def test_flow():
        return cloud_storage_download_blob("bucket", "blob", gcp_credentials, path=path)

    if path is None:
        assert test_flow().result().result() == b"bytes"
    else:
        assert test_flow().result().result() == path


@pytest.mark.parametrize(
    "data",
    [
        "str_data",
        b"bytes_data",
        "/path/somewhere/",
        Path("/path/somewhere/"),
        BytesIO(b"data"),
    ],
)
@pytest.mark.parametrize("blob", [None, "blob"])
def test_cloud_storage_upload_blob(data, blob, gcp_credentials):
    @flow
    def test_flow():
        return cloud_storage_upload_blob(data, "bucket", gcp_credentials, blob=blob)

    if isinstance(data, (str, Path)):
        is_file_path = os.path.exists(data) and os.path.isfile(data)
    else:
        is_file_path = False

    if blob is None and not is_file_path:
        with pytest.raises(ValueError):
            test_flow().result(raise_on_failure=True)
    else:
        if blob is None:
            blob = os.path.basename(data)
        assert test_flow().result().result() == blob


def test_cloud_storage_copy_blob(gcp_credentials):
    @flow
    def test_flow():
        return cloud_storage_copy_blob(
            "source_bucket", "source_blob", "dest_bucket", gcp_credentials
        )

    assert test_flow().result().result() == "dest_bucket"
