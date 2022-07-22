from pathlib import Path

import pytest
from prefect import flow

from prefect_gcp import GcpCredentials

SERVICE_ACCOUNT_FILES = [
    Path(__file__).parent.absolute() / "test_credentials.py",
]
SERVICE_ACCOUNT_FILES.append(str(SERVICE_ACCOUNT_FILES[0]))


@pytest.fixture()
def service_account_info():
    return {"key": "abc", "pass": "pass"}


@pytest.mark.parametrize("service_account_file", SERVICE_ACCOUNT_FILES)
def test_get_credentials_from_service_account_file(
    service_account_file, oauth2_credentials
):
    credentials = GcpCredentials._get_credentials_from_service_account(
        service_account_file=service_account_file
    )
    assert credentials == service_account_file


def test_get_credentials_from_service_account_info(
    service_account_info, oauth2_credentials
):
    credentials = GcpCredentials._get_credentials_from_service_account(
        service_account_info=service_account_info
    )
    assert credentials == service_account_info


def test_get_credentials_from_service_account_none(oauth2_credentials):
    assert GcpCredentials._get_credentials_from_service_account() is None


def test_get_credentials_from_service_account_file_error(oauth2_credentials):
    with pytest.raises(ValueError):
        GcpCredentials._get_credentials_from_service_account(
            service_account_file="~/doesnt/exist"
        )


def test_get_credentials_from_service_account_both_error(
    service_account_info, oauth2_credentials
):
    with pytest.raises(ValueError):
        GcpCredentials._get_credentials_from_service_account(
            service_account_file=SERVICE_ACCOUNT_FILES[0],
            service_account_info=service_account_info,
        )


@pytest.mark.parametrize("override_project", [None, "override_project"])
def test_get_cloud_storage_client(
    override_project, service_account_info, oauth2_credentials, storage_client
):
    @flow
    def test_flow():
        project = "test_project"
        credentials = GcpCredentials(
            service_account_info=service_account_info,
            project=project,
        )
        client = credentials.get_cloud_storage_client(project=override_project)
        assert client.credentials == service_account_info

        if override_project is None:
            assert client.project == project
        else:
            assert client.project == override_project
        return True

    test_flow()
