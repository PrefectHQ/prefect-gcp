import json
from pathlib import Path

import pytest

from prefect_gcp import GcpCredentials

SERVICE_ACCOUNT_FILES = [
    Path(__file__).parent.absolute() / "test_credentials.py",
]
SERVICE_ACCOUNT_FILES.append(str(SERVICE_ACCOUNT_FILES[0]))

SERVICE_ACCOUNT_INFOS = [
    {"key": "abc", "pass": "pass"},
    '{"key": "abc", "pass": "pass"}',
]


@pytest.mark.parametrize("service_account_file", SERVICE_ACCOUNT_FILES)
def test_get_credentials_from_service_account_file(
    service_account_file, oauth2_credentials
):
    credentials = GcpCredentials._get_credentials_from_service_account(
        service_account_file=service_account_file
    )
    assert credentials == service_account_file


@pytest.mark.parametrize("service_account_info", SERVICE_ACCOUNT_INFOS)
def test_get_credentials_from_service_account_info(
    service_account_info, oauth2_credentials
):
    credentials = GcpCredentials._get_credentials_from_service_account(
        service_account_info=service_account_info
    )
    if isinstance(service_account_info, str):
        service_account_info = json.loads(service_account_info)
    assert credentials == service_account_info


def test_get_credentials_from_service_account_none(oauth2_credentials):
    assert GcpCredentials._get_credentials_from_service_account() is None


def test_get_credentials_from_service_account_file_error(oauth2_credentials):
    with pytest.raises(ValueError):
        GcpCredentials._get_credentials_from_service_account(
            service_account_file="~/doesnt/exist"
        )


def test_get_credentials_from_service_account_both_error(oauth2_credentials):
    with pytest.raises(ValueError):
        GcpCredentials._get_credentials_from_service_account(
            service_account_file=SERVICE_ACCOUNT_FILES[0],
            service_account_info=SERVICE_ACCOUNT_INFOS[0],
        )


@pytest.mark.parametrize("method_project", [None, "override_project"])
def test_get_cloud_storage_client(method_project, oauth2_credentials, storage_client):
    project = "test_project"
    client = GcpCredentials(
        service_account_info=SERVICE_ACCOUNT_INFOS[0], project=project
    ).get_cloud_storage_client(project=method_project)
    assert client.credentials == SERVICE_ACCOUNT_INFOS[0]
    if method_project is None:
        assert client.project == project
    else:
        assert client.project == method_project


@pytest.mark.parametrize("method_project", [None, "override_project"])
def test_get_bigquery_client(method_project, oauth2_credentials, storage_client):
    project = "test_project"
    client = GcpCredentials(
        service_account_info=SERVICE_ACCOUNT_INFOS[0], project=project
    ).get_bigquery_client(project=method_project)
    assert client.credentials == SERVICE_ACCOUNT_INFOS[0]
    if method_project is None:
        assert client.project == project
    else:
        assert client.project == method_project
