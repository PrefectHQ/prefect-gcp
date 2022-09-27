import json
import os
from pathlib import Path, PosixPath

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


@pytest.mark.parametrize("service_account_file", SERVICE_ACCOUNT_FILES)
def test_get_credentials_from_service_account_file(
    service_account_file, oauth2_credentials
):
    """Expected behavior:
    `service_account_file` is typed as a path, so we expect either input
    to be a PosixPath.

    In our conftest, we define a fixture `oauth2_credentials` that patches
    GCP's Credential methods to return its input.
    We expect our `get_credentials_from_service_account`
    method to call GCP's method with the path we pass in.
    """
    credentials = GcpCredentials(
        service_account_file=service_account_file, project="my-project"
    ).get_credentials_from_service_account()
    assert isinstance(credentials, PosixPath)
    assert credentials == Path(service_account_file).expanduser()


def test_get_credentials_from_service_account_info(
    service_account_info, oauth2_credentials
):
    credentials = GcpCredentials(
        service_account_info=service_account_info, project="my-project"
    ).get_credentials_from_service_account()
    if isinstance(service_account_info, str):
        service_account_info = json.loads(service_account_info)
    for key, value in service_account_info.items():
        assert getattr(credentials, key) == value


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


def test_block_initialization(service_account_info, oauth2_credentials):
    gcp_credentials = GcpCredentials(service_account_info=service_account_info)
    assert gcp_credentials.project == "my_project"


def test_block_initialization_project_specified(
    service_account_info, oauth2_credentials
):
    gcp_credentials = GcpCredentials(
        service_account_info=service_account_info, project="overrided_project"
    )
    assert gcp_credentials.project == "overrided_project"


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
        for key, value in expected.items():
            assert getattr(client.credentials, key) == value

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
        configs = self.credentials.dict()
        for key in Block().dict():
            configs.pop(key, None)
        return configs


class MockCliProfile(Block):
    target_configs: MockTargetConfigs

    def get_profile(self):
        profile = {
            "name": {
                "outputs": {"target": self.target_configs.get_configs()},
            },
        }
        return profile


def test_credentials_is_able_to_serialize_back(monkeypatch, service_account_info):
    @task
    def test_task(mock_cli_profile):
        return mock_cli_profile.get_profile()

    @flow
    def test_flow():
        gcp_credentials = GcpCredentials(
            service_account_info=service_account_info, project="my-project"
        )
        mock_target_configs = MockTargetConfigs(credentials=gcp_credentials)
        mock_cli_profile = MockCliProfile(target_configs=mock_target_configs)
        task_result = test_task(mock_cli_profile)
        return task_result

    expected = {
        "name": {
            "outputs": {
                "target": {
                    "project": "my-project",
                    "service_account_file": None,
                    "service_account_info": {
                        "project_id": "my_project",
                        "token_uri": "my-token-uri",
                        "client_email": "my-client-email",
                        "private_key": "my-private-key",
                    },
                }
            }
        }
    }
    assert test_flow() == expected
