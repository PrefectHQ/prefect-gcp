import os
from pathlib import Path

import pytest
from prefect import flow

from prefect_gcp.cloud_storage import (
    cloud_storage_create_bucket,
    cloud_storage_download_blob,
    cloud_storage_get_bucket,
    cloud_storage_upload_blob,
)


def test_cloud_storage_create_bucket(gcp_credentials):
    bucket = "expected"

    @flow
    def test_flow():
        return cloud_storage_create_bucket(bucket, gcp_credentials)

    assert test_flow().result().result() == bucket


@pytest.mark.parametrize("project", [None, "override_project"])
@pytest.mark.parametrize("location", [None, "override_location"])
def test_cloud_storage_get_bucket(project, location, gcp_credentials):
    bucket = "expected"

    @flow
    def test_flow():
        return cloud_storage_get_bucket(
            bucket, gcp_credentials, project=project, location=location
        )

    assert test_flow().result().result().bucket == bucket
    assert project == project
    assert location == location


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
    "data", ["str_data", b"bytes_data", "/path/somewhere/", Path("/path/somewhere/")]
)
@pytest.mark.parametrize("blob", [None, "blob"])
def test_cloud_storage_upload_blob(data, blob, gcp_credentials):
    @flow
    def test_flow():
        return cloud_storage_upload_blob(data, "bucket", gcp_credentials, blob=blob)

    is_file_path = os.path.exists(data) and os.path.isfile(data)
    if blob is None and not is_file_path:
        with pytest.raises(ValueError):
            test_flow().result(raise_on_failure=True)
    else:
        if blob is None:
            blob = os.path.basename(data)
        assert test_flow().result().result() == blob
