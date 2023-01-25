import os
from io import BytesIO
from pathlib import Path, PurePosixPath
from uuid import UUID

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
        return cloud_storage_create_bucket(
            bucket, gcp_credentials, location=location, timeout=10
        )

    assert test_flow() == "expected"


@pytest.mark.parametrize("path", ["file.txt", Path("file.txt")])
def test_cloud_storage_download_blob_to_file(tmp_path, path, gcp_credentials):
    input_path = tmp_path / path
    if isinstance(path, str):
        input_path = str(input_path)

    @flow
    def test_flow():
        return cloud_storage_download_blob_to_file(
            "bucket", "blob", input_path, gcp_credentials, timeout=10
        )

    assert test_flow() == input_path
    assert Path(input_path).exists()


def test_cloud_storage_download_blob_as_bytes(gcp_credentials):
    @flow
    def test_flow():
        return cloud_storage_download_blob_as_bytes(
            "bucket", "blob", gcp_credentials, timeout=10
        )

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
            file, "bucket", blob, gcp_credentials, content_type=content_type, timeout=10
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
            data, "bucket", "blob", gcp_credentials, timeout=10
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
            timeout=10,
        )

    if dest_blob is None:
        assert test_flow() == "source_blob"
    else:
        assert test_flow() == "dest_blob"


class TestGcsBucket:
    @pytest.fixture(params=["", "base_folder"])
    def gcs_bucket(self, gcp_credentials, request):
        return GcsBucket(
            bucket="bucket",
            gcp_credentials=gcp_credentials,
            bucket_folder=request.param,
        )

    def test_bucket_folder_suffix(self, gcs_bucket):
        bucket_folder = gcs_bucket.bucket_folder
        if bucket_folder:
            assert bucket_folder.endswith("/")
        else:
            assert bucket_folder == ""

    @pytest.mark.parametrize("path", [None, "subpath"])
    def test_resolve_path(self, gcs_bucket, path):
        actual = gcs_bucket._resolve_path(path)
        bucket_folder = gcs_bucket.bucket_folder
        if path is None:
            dirname, filename = os.path.split(actual)
            assert dirname == bucket_folder.rstrip("/")
            assert UUID(filename, version=4)
        else:
            expected = str(PurePosixPath(bucket_folder) / path)
            assert actual == expected

    def test_read_path(self, gcs_bucket):
        assert gcs_bucket.read_path("blob") == b"bytes"

    def test_write_path(self, gcs_bucket):
        bucket_folder = gcs_bucket.bucket_folder
        assert gcs_bucket.write_path("blob", b"bytes_data") == f"{bucket_folder}blob"

    @pytest.mark.parametrize("from_path", [None, "base_folder"])
    @pytest.mark.parametrize("local_path", [None, "local_path"])
    def test_get_directory(self, gcs_bucket, tmp_path, from_path, local_path):
        os.chdir(tmp_path)

        local_path = None
        assert (
            gcs_bucket.get_directory(from_path=from_path, local_path=local_path) is None
        )
        if local_path is None:
            local_path = os.path.abspath(".")

        if from_path is None:
            from_path = gcs_bucket.bucket_folder

        file_path = os.path.join(local_path, from_path)
        assert os.path.exists(file_path)

    @pytest.mark.parametrize("to_path", [None, "to_path"])
    @pytest.mark.parametrize("ignore", [True, False])
    def test_put_directory(self, gcs_bucket, tmp_path, to_path, ignore):
        local_path = tmp_path / "a_directory"
        local_path.mkdir()

        (local_path / "abc.html").write_text("<div>abc</div>")
        (local_path / "cab.txt").write_text("cab")
        (local_path / "some_dir").mkdir()

        expected = 2
        if ignore:
            ignore_file = tmp_path / "ignore.txt"
            ignore_file.write_text("*.html")
            expected -= 1
        else:
            ignore_file = None

        actual = gcs_bucket.put_directory(
            local_path=local_path, to_path=to_path, ignore_file=ignore_file
        )
        assert actual == expected

    def test_get_bucket(self, gcs_bucket):
        bucket = gcs_bucket.get_bucket()
        assert bucket.name == "my-bucket"

    @pytest.fixture
    def gcs_bucket_no_bucket_folder(self, gcp_credentials):
        return GcsBucket(
            bucket="bucket",
            gcp_credentials=gcp_credentials,
        )

    @pytest.fixture(params=["base_folder", "base_folder/"])
    def gcs_bucket_with_bucket_folder(self, gcp_credentials, request):
        return GcsBucket(
            bucket="bucket",
            gcp_credentials=gcp_credentials,
            bucket_folder=request.param,
        )

    def test_list_folders(self, gcs_bucket_with_bucket_folder):
        blobs = gcs_bucket_with_bucket_folder.list_folders()
        assert len(blobs) == 1
        assert blobs[0] == "sub_folder"
    def test_list_blobs(self, gcs_bucket_no_bucket_folder):
        blobs = gcs_bucket_no_bucket_folder.list_blobs(folder="base_folder/")
        assert len(blobs) == 2
        assert blobs[0].name == "base_folder/nested_blob.txt"
        assert blobs[1].name == "base_folder/sub_folder/nested_blob.txt"

    def test_list_blobs_with_bucket_folder(self, gcs_bucket_with_bucket_folder):
        # test without setting folder
        blobs = gcs_bucket_with_bucket_folder.list_blobs()
        assert len(blobs) == 2
        assert blobs[0].name == "base_folder/nested_blob.txt"
        assert blobs[1].name == "base_folder/sub_folder/nested_blob.txt"

        # now test with sub_folder
        blobs = gcs_bucket_with_bucket_folder.list_blobs(folder="sub_folder")
        assert len(blobs) == 1
        assert blobs[0].name == "base_folder/sub_folder/nested_blob.txt"

    def test_list_blobs_root_folder(self, gcs_bucket_no_bucket_folder):
        blobs = gcs_bucket_no_bucket_folder.list_blobs(folder="")
        assert len(blobs) == 3
        assert blobs[0].name == "blob.txt"
        assert blobs[1].name == "base_folder/nested_blob.txt"
        assert blobs[2].name == "base_folder/sub_folder/nested_blob.txt"

    def test_download_object_to_path_default(
        self, gcs_bucket_with_bucket_folder, tmp_path
    ):
        os.chdir(tmp_path)
        from_path = tmp_path / "from_path"
        output_to_path = gcs_bucket_with_bucket_folder.download_object_to_path(
            from_path
        )
        assert isinstance(output_to_path, Path)
        assert output_to_path == tmp_path / "from_path"
        assert output_to_path.read_text() == "abcdef"

    @pytest.mark.parametrize("type_", [str, Path])
    def test_download_object_to_path_set_to_path(
        self, gcs_bucket_with_bucket_folder, tmp_path, type_
    ):
        from_path = tmp_path / "from_path"
        to_path = type_(tmp_path / "to_path")
        output_to_path = gcs_bucket_with_bucket_folder.download_object_to_path(
            from_path, to_path
        )
        assert isinstance(to_path, type_)
        assert output_to_path == Path(to_path)
        assert output_to_path.read_text() == "abcdef"

    def test_download_object_to_file_object_bytesio(
        self, gcs_bucket_with_bucket_folder, tmp_path
    ):
        from_path = tmp_path / "from_path"
        with BytesIO() as buf:
            to_file_object = (
                gcs_bucket_with_bucket_folder.download_object_to_file_object(
                    from_path, buf
                )
            )
            assert to_file_object.getvalue() == b"abcdef"

    def test_download_object_to_file_object_bufferedwriter(
        self, gcs_bucket_with_bucket_folder, tmp_path
    ):
        from_path = tmp_path / "from_path"
        to_path = tmp_path / "to_path"
        with open(to_path, "wb") as f:
            gcs_bucket_with_bucket_folder.download_object_to_file_object(from_path, f)
        assert to_path.read_text() == "abcdef"

    def test_download_folder_to_path_default_no_bucket_folder(
        self, gcs_bucket_no_bucket_folder, tmp_path
    ):
        os.chdir(tmp_path)
        from_path = "base_folder"
        output_to_path = gcs_bucket_no_bucket_folder.download_folder_to_path(from_path)
        assert output_to_path == tmp_path
        assert (output_to_path / "nested_blob.txt").read_text() == "abcdef"
        assert (
            output_to_path / "sub_folder" / "nested_blob.txt"
        ).read_text() == "abcdef"

    def test_download_folder_to_path_default_with_bucket_folder(
        self, gcs_bucket_with_bucket_folder, tmp_path
    ):
        os.chdir(tmp_path)
        from_path = ""
        output_to_path = gcs_bucket_with_bucket_folder.download_folder_to_path(
            from_path
        )
        assert output_to_path == tmp_path
        assert (output_to_path / "nested_blob.txt").read_text() == "abcdef"
        assert (
            output_to_path / "sub_folder" / "nested_blob.txt"
        ).read_text() == "abcdef"

    def test_download_folder_to_path_no_bucket_folder(
        self, gcs_bucket_no_bucket_folder, tmp_path
    ):
        from_path = "base_folder"
        to_path = tmp_path / "to_path"
        output_to_path = gcs_bucket_no_bucket_folder.download_folder_to_path(
            from_path, to_path
        )
        assert output_to_path == to_path
        assert (to_path / "nested_blob.txt").read_text() == "abcdef"
        assert (to_path / "sub_folder" / "nested_blob.txt").read_text() == "abcdef"

    def test_download_folder_to_path_nested(
        self, gcs_bucket_no_bucket_folder, tmp_path
    ):
        from_path = "base_folder/sub_folder"
        to_path = tmp_path / "to_path"
        output_to_path = gcs_bucket_no_bucket_folder.download_folder_to_path(
            from_path, to_path
        )
        assert output_to_path == to_path
        assert (to_path / "nested_blob.txt").read_text() == "abcdef"

    def test_download_folder_to_path_nested_with_bucket_folder(
        self, gcs_bucket_with_bucket_folder, tmp_path
    ):
        from_path = "sub_folder"
        to_path = tmp_path / "to_path"
        output_to_path = gcs_bucket_with_bucket_folder.download_folder_to_path(
            from_path, to_path
        )
        assert output_to_path == to_path
        assert (to_path / "nested_blob.txt").read_text() == "abcdef"

    def test_upload_from_path_default_with_bucket_folder(
        self, gcs_bucket_with_bucket_folder, tmp_path
    ):
        from_path = tmp_path / "from_path"
        from_path.write_text("abcdef")
        output_to_path = gcs_bucket_with_bucket_folder.upload_from_path(from_path)
        assert output_to_path == f"base_folder/{from_path.name}"

    def test_upload_from_path_set_with_bucket_folder(
        self, gcs_bucket_with_bucket_folder, tmp_path
    ):
        from_path = tmp_path / "from_path"
        from_path.write_text("abcdef")
        to_path = "to_path"
        output_to_path = gcs_bucket_with_bucket_folder.upload_from_path(
            from_path, to_path
        )
        assert output_to_path == f"base_folder/{to_path}"

    def test_upload_from_file_object_bytesio_with_bucket_folder(
        self, gcs_bucket_with_bucket_folder
    ):
        to_path = "to_path"
        with BytesIO() as buf:
            output_to_path = gcs_bucket_with_bucket_folder.upload_from_file_object(
                buf, to_path
            )
        assert output_to_path == f"base_folder/{to_path}"

    def test_upload_from_file_object_bufferedwriter_with_bucket_folder(
        self, gcs_bucket_with_bucket_folder, tmp_path
    ):
        to_path = "to_path"
        from_path = tmp_path / "from_path"
        with open(from_path, "wb") as f:
            output_to_path = gcs_bucket_with_bucket_folder.upload_from_file_object(
                f, to_path
            )
        assert output_to_path == f"base_folder/{to_path}"

    def test_upload_from_folder_default_no_bucket_folder(
        self, gcs_bucket_no_bucket_folder, tmp_path
    ):
        from_path = tmp_path / "from_path"
        from_path.mkdir()
        (from_path / "abc.html").write_text("<div>abc</div>")
        (from_path / "cab.txt").write_text("cab")
        (from_path / "some_dir").mkdir()

        output_to_path = gcs_bucket_no_bucket_folder.upload_from_folder(from_path)
        assert output_to_path == ""

    def test_upload_from_folder_default_with_bucket_folder(
        self, gcs_bucket_with_bucket_folder, tmp_path
    ):
        from_path = tmp_path / "from_path"
        from_path.mkdir()
        (from_path / "abc.html").write_text("<div>abc</div>")
        (from_path / "cab.txt").write_text("cab")
        (from_path / "some_dir").mkdir()

        output_to_path = gcs_bucket_with_bucket_folder.upload_from_folder(from_path)
        assert output_to_path == "base_folder"

    def test_upload_from_folder_set_to_path_no_bucket_folder(
        self, gcs_bucket_no_bucket_folder, tmp_path
    ):
        from_path = tmp_path / "from_path"
        from_path.mkdir()
        (from_path / "abc.html").write_text("<div>abc</div>")
        (from_path / "cab.txt").write_text("cab")
        (from_path / "some_dir").mkdir()

        output_to_path = gcs_bucket_no_bucket_folder.upload_from_folder(
            from_path, "to_path"
        )
        print(output_to_path)
        assert output_to_path == "to_path"

    def test_upload_from_folder_set_to_path_with_bucket_folder(
        self, gcs_bucket_with_bucket_folder, tmp_path
    ):
        from_path = tmp_path / "from_path"
        from_path.mkdir()
        (from_path / "abc.html").write_text("<div>abc</div>")
        (from_path / "cab.txt").write_text("cab")
        (from_path / "some_dir").mkdir()

        output_to_path = gcs_bucket_with_bucket_folder.upload_from_folder(
            from_path, "to_path"
        )
        assert output_to_path == "base_folder/to_path"
