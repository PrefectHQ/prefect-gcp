import json
import os
from pathlib import Path

import pytest
from prefect import flow, task
from prefect.blocks.core import Block

from prefect_gcp import GcpCredentials


def _get_first_file_in_root():
    for path in os.listdir(os.path.expanduser("~")):
        if os.path.isfile(os.path.join(os.path.expanduser("~"), path)):
            return os.path.join("~", path)


SERVICE_ACCOUNT_FILES = [
    Path(__file__).parent.absolute() / "test_credentials.py",
]
SERVICE_ACCOUNT_FILES.append(str(SERVICE_ACCOUNT_FILES[0]))
SERVICE_ACCOUNT_FILES.append(_get_first_file_in_root())
SERVICE_ACCOUNT_FILES.append(os.path.expanduser(_get_first_file_in_root()))


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
    credentials = GcpCredentials._get_credentials_from_service_account(
        service_account_file=service_account_file
    )
    assert str(credentials) == os.path.expanduser(service_account_file)


def test_get_credentials_from_service_account_info(
    service_account_info_dict, oauth2_credentials
):
    credentials = GcpCredentials._get_credentials_from_service_account(
        service_account_info=service_account_info_dict
    )
    assert credentials == service_account_info_dict


def test_get_credentials_from_service_account_none(oauth2_credentials):
    assert GcpCredentials._get_credentials_from_service_account() is None


def test_get_credentials_from_service_account_file_error(oauth2_credentials):
    with pytest.raises(ValueError):
        GcpCredentials._get_credentials_from_service_account(
            service_account_file="~/doesnt/exist"
        )


def test_get_credentials_from_service_account_both_error(
    service_account_info_dict, oauth2_credentials
):
    with pytest.raises(ValueError):
        GcpCredentials._get_credentials_from_service_account(
            service_account_file=SERVICE_ACCOUNT_FILES[0],
            service_account_info=service_account_info_dict,
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
