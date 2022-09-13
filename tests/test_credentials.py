import json
from pathlib import Path, PosixPath
from unittest.mock import Mock

import pytest
from prefect import flow, task
from prefect.blocks.core import Block

from prefect_gcp import GcpCredentials

SERVICE_ACCOUNT_FILES = [
    Path(__file__).parent.absolute() / "test_credentials.py",
]
SERVICE_ACCOUNT_FILES.append(str(SERVICE_ACCOUNT_FILES[0]))


@pytest.fixture()
def service_account_info_dict():
    _service_account_info = {"key": "abc", "pass": "pass"}
    return _service_account_info


@pytest.fixture()
def service_account_info_json(service_account_info_dict):
    _service_account_info = json.dumps(service_account_info_dict)
    return _service_account_info


@pytest.fixture(params=["service_account_info_dict", "service_account_info_json"])
def service_account_info(request):
    return request.getfixturevalue(request.param)


@pytest.mark.parametrize("service_account_file", SERVICE_ACCOUNT_FILES)
def test_get_credentials_from_service_account_file(
    service_account_file, oauth2_credentials
):
    """Expected behavior: 
    - `service_account_file` is typed as a path, so we expect either input 
    to be a PosixPath. 
    - In our conftest, we define a fixture `oauth2_credentials` that patches 
    GCP's Credential methods to return its input. We expect our `get_credentials_from_service_account`
    method to call GCP's method with the path we pass in.
    """
    credentials = GcpCredentials(
        service_account_file=service_account_file
    ).get_credentials_from_service_account()
    assert isinstance(credentials, PosixPath)
    assert str(credentials) == str(service_account_file)


def test_get_credentials_from_service_account_info(
    service_account_info_dict, oauth2_credentials
):
    credentials = GcpCredentials(
        service_account_info=service_account_info_dict
    ).get_credentials_from_service_account()
    assert credentials == service_account_info_dict


def test_errors_without_credential_file_or_info():
    with pytest.raises(ValueError):
        GcpCredentials().get_credentials_from_service_account()


def test_get_credentials_from_service_account_file_error(oauth2_credentials):
    with pytest.raises(ValueError):
        GcpCredentials(
            service_account_file="~/doesnt/exist"
        ).get_credentials_from_service_account()


def test_get_credentials_from_service_account_both_error(
    service_account_info_dict, oauth2_credentials
):
    with pytest.raises(ValueError):
        GcpCredentials(
            service_account_file=SERVICE_ACCOUNT_FILES[0],
            service_account_info=service_account_info_dict,
        ).get_credentials_from_service_account()


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
        if isinstance(service_account_info, str):
            expected = json.loads(service_account_info)
        else:
            expected = service_account_info
        assert client.credentials == expected

        if override_project is None:
            assert client.project == project
        else:
            assert client.project == override_project
        return True

    test_flow()

@pytest.mark.parametrize("override_project", [None, "override_project"])
def test_project_id(
    override_project, service_account_info, monkeypatch
    ):
    MOCK_PROJECT_VALUE = 'my-project-id'
    def return_mock_credentials(*args, **kwargs):
        m = Mock()
        m.project_id = MOCK_PROJECT_VALUE
        return m

    monkeypatch.setattr(
        "prefect_gcp.credentials.GcpCredentials.get_credentials_from_service_account",
        return_mock_credentials
    )
    credentials = GcpCredentials(
        service_account_info=service_account_info,
        project=override_project,
    )
    if override_project:
        assert credentials.project_id == override_project
    else:
        assert credentials.project_id == MOCK_PROJECT_VALUE


class MockTargetConfigs(Block):
    credentials: GcpCredentials

    def get_configs(self):
        """
        Returns the dbt configs, likely used eventually for writing to profiles.yml.
        Returns:
            A configs JSON.
        """
        return self.credentials.dict()


class MockCliProfile(Block):
    target_configs: MockTargetConfigs

    def get_profile(self):
        profile = {
            "name": {
                "outputs": {"target": self.target_configs.get_configs()},
            },
        }
        return profile


def test_credentials_is_able_to_serialize_back(service_account_info):
    @task
    def test_task(mock_cli_profile):
        return mock_cli_profile.get_profile()

    @flow
    def test_flow():
        gcp_credentials = GcpCredentials(service_account_info=service_account_info)
        mock_target_configs = MockTargetConfigs(credentials=gcp_credentials)
        mock_cli_profile = MockCliProfile(target_configs=mock_target_configs)
        task_result = test_task(mock_cli_profile)
        return task_result

    expected = {
        "name": {
            "outputs": {
                "target": {
                    "project": None,
                    "service_account_file": None,
                    "service_account_info": {"key": "abc", "pass": "pass"},
                }
            }
        }
    }
    assert test_flow() == expected
