from unittest.mock import MagicMock

import pytest


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


@pytest.fixture
def storage_client(monkeypatch):
    monkeypatch.setattr("prefect_gcp.credentials.StorageClient", CloudStorageClient)


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


@pytest.fixture
def gcp_credentials():
    gcp_credentials_mock = MagicMock()
    gcp_credentials_mock.get_cloud_storage_client.return_value = CloudStorageClient()
    gcp_credentials_mock.get_bigquery_client.return_value = BigQueryClient()
    return gcp_credentials_mock
