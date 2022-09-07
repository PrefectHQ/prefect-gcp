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
    CredentialsMock.from_service_account_info.side_effect = lambda json: json
    CredentialsMock.from_service_account_file.side_effect = lambda file: file
    monkeypatch.setattr("prefect_gcp.credentials.Credentials", CredentialsMock)


class CloudStorageClient:
    def __init__(self, credentials=None, project=None):
        self.credentials = credentials
        self.project = project

    def create_bucket(self, bucket, location=None):
        return {"bucket": bucket, "location": location}

    def get_bucket(self, bucket):
        blob_obj = MagicMock()
        blob_obj.download_as_bytes.return_value = b"bytes"
        bucket_obj = MagicMock(bucket=bucket)
        bucket_obj.blob.side_effect = lambda blob, **kwds: blob_obj
        return bucket_obj

    def list_blobs(self, bucket):
        blob_obj = MagicMock(name="blob")
        blob_directory = MagicMock(name="directory/")
        return [blob_obj, blob_directory]


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
def gcp_credentials():
    gcp_credentials_mock = GcpCredentials(project="gcp_credentials_project")
    gcp_credentials_mock.get_cloud_storage_client = (
        lambda *args, **kwargs: CloudStorageClient()
    )
    gcp_credentials_mock.get_bigquery_client = lambda *args, **kwargs: BigQueryClient()
    gcp_credentials_mock.get_secret_manager_client = (
        lambda *args, **kwargs: SecretManagerClient()
    )
    return gcp_credentials_mock
