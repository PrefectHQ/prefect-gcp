import json
from pathlib import Path

import pytest

from prefect_gcp import GCPCredentials

SERVICE_ACCOUNT_JSONS = [
    None,
    {"key": "abc", "pass": "pass"},
    '{"key": "abc", "pass": "pass"}',
    Path(__file__).parent.absolute() / "test_credentials.py",
]


@pytest.mark.parametrize("service_account_json", SERVICE_ACCOUNT_JSONS)
def test_get_credentials_from_service_account(service_account_json, oauth2_credentials):
    if isinstance(service_account_json, str) and "{" in service_account_json:
        service_account_json = json.loads(service_account_json)
    elif isinstance(service_account_json, Path):
        service_account_json = str(service_account_json)
    credentials = GCPCredentials._get_credentials_from_service_account(
        service_account_json
    )
    assert credentials == service_account_json


@pytest.mark.parametrize("service_account_json", ["~/bad/path", "$HOME/bad/too"])
def test_get_credentials_from_service_account_error(
    service_account_json, oauth2_credentials
):
    with pytest.raises(ValueError):
        GCPCredentials._get_credentials_from_service_account(service_account_json)


@pytest.mark.parametrize("method_project", [None, "override_project"])
def test_get_cloud_storage_client(method_project, oauth2_credentials, storage_client):
    project = "test_project"
    client = GCPCredentials(
        service_account_json=SERVICE_ACCOUNT_JSONS[1], project=project
    ).get_cloud_storage_client(project=method_project)
    assert client.credentials == SERVICE_ACCOUNT_JSONS[1]
    if method_project is None:
        assert client.project == project
    else:
        assert client.project == method_project
