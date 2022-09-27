import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from google.cloud.exceptions import NotFound
from prefect.testing.utilities import prefect_test_harness

from prefect_gcp.credentials import GcpCredentials


@pytest.fixture(scope="session", autouse=True)
def prefect_db():
    with prefect_test_harness():
        yield


@pytest.fixture
def oauth2_credentials(monkeypatch):
    CredentialsMock = MagicMock()
    CredentialsMock.from_service_account_info.side_effect = (
        lambda json, scopes: MagicMock(scopes=scopes, **json)
    )
    CredentialsMock.from_service_account_file.side_effect = lambda file, scopes: file
    monkeypatch.setattr("prefect_gcp.credentials.Credentials", CredentialsMock)


class Blob:
    def __init__(self, name):
        self.name = name


class CloudStorageClient:
    def __init__(self, credentials=None, project=None):
        self.credentials = credentials
        self.project = project

    def create_bucket(self, bucket, location=None, **create_kwargs):
        return {"bucket": bucket, "location": location, **create_kwargs}

    def get_bucket(self, bucket):
        blob_obj = MagicMock()
        blob_obj.download_as_bytes.return_value = b"bytes"
        blob_obj.download_to_filename.side_effect = lambda path, **kwargs: Path(
            path
        ).write_text("abcdef")
        bucket_obj = MagicMock(bucket=bucket)
        bucket_obj.blob.side_effect = lambda blob, **kwds: blob_obj
        return bucket_obj

    def list_blobs(self, bucket, prefix=None):
        blob_obj = Blob(name="blob.txt")
        blob_directory = Blob(name="directory/")
        nested_blob_obj = Blob(name="base_folder/nested_blob.txt")
        double_nested_blob_obj = Blob(name="base_folder/base_folder/nested_blob.txt")
        blobs = [blob_obj, blob_directory, nested_blob_obj, double_nested_blob_obj]
        for blob in blobs:
            if prefix and not blob.name.startswith(prefix):
                blobs.remove(blob)
        return blobs


@pytest.fixture
def storage_client(monkeypatch):
    monkeypatch.setattr("prefect_gcp.credentials.StorageClient", CloudStorageClient)


class LoadJob:
    def __init__(self, output, *args, **kwargs):
        self.output = output
        self._client = "client"
        self._completion_lock = "completion_lock"


class BigQueryClient:
    def __init__(self, credentials=None, project=None):
        self.credentials = credentials
        self.project = project

    def query(self, query, **kwargs):
        response = MagicMock()
        result = MagicMock()
        result.__iter__.return_value = [query]
        result.to_dataframe.return_value = f"dataframe_{query}"
        result.kwargs = kwargs
        response.result.return_value = result
        response.total_bytes_processed = 10
        return response

    def dataset(self, dataset):
        return MagicMock()

    def table(self, table):
        return MagicMock()

    def get_dataset(self, dataset):
        dataset_obj = MagicMock(table=MagicMock())
        return dataset_obj

    def create_dataset(self, dataset):
        return self.get_dataset(dataset)

    def get_table(self, table):
        raise NotFound("testing")

    def create_table(self, table):
        return table

    def insert_rows_json(self, table, json_rows):
        return json_rows

    def load_table_from_uri(self, uri, *args, **kwargs):
        output = MagicMock()
        output.result.return_value = LoadJob(uri)
        return output

    def load_table_from_file(self, *args, **kwargs):
        output = MagicMock()
        output.result.return_value = LoadJob("file")
        return output


class SecretManagerClient:
    def __init__(self, credentials=None, project=None):
        self.credentials = credentials
        self.project = project

    def create_secret(self, parent=None, secret_id=None, **kwds):
        response = MagicMock()
        response.name = secret_id
        return response

    def add_secret_version(self, parent, payload, **kwds):
        response = MagicMock()
        response.name = payload["data"]
        return response

    def access_secret_version(self, name, **kwds):
        response = MagicMock()
        payload = MagicMock()
        payload.data = f"{name}".encode()
        response.payload = payload
        return response

    def delete_secret(self, name, **kwds):
        return name

    def destroy_secret_version(self, name, **kwds):
        return name


@pytest.fixture
def gcp_credentials(monkeypatch):
    google_auth_mock = MagicMock()
    google_auth_mock.default.side_effect = lambda: (MagicMock(), MagicMock())
    monkeypatch.setattr("google.auth", google_auth_mock)
    gcp_credentials_mock = GcpCredentials(project="gcp_credentials_project")
    google_auth_mock.default.assert_called_with()
    gcp_credentials_mock.get_cloud_storage_client = (
        lambda *args, **kwargs: CloudStorageClient()
    )
    gcp_credentials_mock.get_bigquery_client = lambda *args, **kwargs: BigQueryClient()
    gcp_credentials_mock.get_secret_manager_client = (
        lambda *args, **kwargs: SecretManagerClient()
    )
    return gcp_credentials_mock


@pytest.fixture()
def service_account_info_dict(monkeypatch):
    monkeypatch.setattr(
        "google.auth.crypt._cryptography_rsa.serialization.load_pem_private_key",
        lambda *args, **kwargs: args[0],
    )
    _service_account_info = {
        "project_id": "my_project",
        "token_uri": "my-token-uri",
        "client_email": "my-client-email",
        "private_key": "my-private-key",
    }
    return _service_account_info


@pytest.fixture()
def service_account_info_json(service_account_info_dict):
    _service_account_info = json.dumps(service_account_info_dict)
    return _service_account_info


@pytest.fixture(params=["service_account_info_dict", "service_account_info_json"])
def service_account_info(request):
    return request.getfixturevalue(request.param)
