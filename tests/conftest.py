from unittest.mock import MagicMock

import pytest


@pytest.fixture
def oauth2_credentials(monkeypatch):
    OAuth2Credentials = MagicMock()
    OAuth2Credentials.from_service_account_info.side_effect = lambda json: json
    OAuth2Credentials.from_service_account_file.side_effect = lambda file: file
    monkeypatch.setattr("prefect_gcp.credentials.Credentials", OAuth2Credentials)


class CloudStorageClient:
    def __init__(self, credentials=None, project=None):
        self.credentials = credentials
        self.project = project


@pytest.fixture
def storage_client(monkeypatch):
    monkeypatch.setattr("prefect_gcp.credentials.Client", CloudStorageClient)
