from unittest.mock import Mock

import pydantic
import pytest
from prefect.settings import (
    PREFECT_API_KEY,
    PREFECT_API_URL,
    PREFECT_PROFILES_PATH,
    temporary_settings,
)

from prefect_gcp.cloud_run_v2 import CloudRunJobResultV2, CloudRunJobV2, JobV2
from prefect_gcp.credentials import GcpCredentials

jobs_return_value = {
    "name": "test-job-name",
    "uid": "uid-123",
    "generation": "1",
    "labels": {},
    "createTime": "create-time",
    "updateTime": "update-time",
    "deleteTime": "delete-time",
    "expireTime": "expire-time",
    "creator": "creator",
    "lastModifier": "last-modifier",
    "client": "client",
    "clientVersion": "client-version",
    "launchStage": "launch-stage",
    "binaryAuthorization": {},
    "template": {},
    "observedGeneration": "1",
    "terminalCondition": {},
    "conditions": [],
    "executionCount": 1,
    "latestCreationExecution": {},
    "reconciling": True,
    "satisfiesPzs": False,
    "etag": "etag-123",
}


@pytest.fixture
def mock_client(monkeypatch, mock_credentials):
    m = Mock(name="MockClient")

    def mock_enter(m, *args, **kwargs):
        return m

    def mock_exit(m, *args, **kwargs):
        pass

    m.__enter__ = mock_enter
    m.__exit__ = mock_exit

    def get_mock_client(*args, **kwargs):
        return m

    monkeypatch.setattr(
        "prefect_gcp.cloud_run_v2.CloudRunJobV2._get_client",
        get_mock_client,
    )

    return m


def list_mock_calls(mock_client, assigned_calls=0):
    calls = []
    for call in mock_client.mock_calls:
        # mock `call.jobs().get()` results in two calls: `call.jobs()` and
        # `call.jobs().get()`, so we want to remove the first, smaller
        # call.
        if len(str(call).split(".")) > 2:
            calls.append(str(call))
    # assigning a return value to a call results in initial
    # mock calls which are not actually made
    actual_calls = calls[assigned_calls:]

    return actual_calls


class TestJobV2:
    @pytest.mark.parametrize(
        "terminal_condition,expected_value",
        [
            (
                {
                    "type": "Ready",
                    "state": "CONDITION_SUCCEEDED",
                    "lastTransitionTime": "2023-07-28T20:10:14.631744Z",
                },
                True,
            ),
            ({}, False),
        ],
    )
    def test_is_ready(self, terminal_condition, expected_value):
        """
        tests that the is_ready function works correctly when given the
        CONDITION_SUCCEEDED input or receives an empty dictionary
        """
        job = JobV2(
            name="test-name",
            uid="123-uuid",
            generation="1",
            create_time="create-time",
            update_time="update-time",
            launch_stage="launch-stage",
            template={},
            terminal_condition=terminal_condition,
            latest_created_execution={},
            etag="etag-123",
        )

        assert job.is_ready() == expected_value

    @pytest.mark.parametrize(
        "terminal_condition,expected_value",
        [
            ({}, {}),
            (
                {
                    "type": "Ready",
                    "state": "CONDITION_SUCCEEDED",
                    "lastTransitionTime": "2023-07-28T20:10:14.631744Z",
                },
                {
                    "type": "Ready",
                    "state": "CONDITION_SUCCEEDED",
                    "lastTransitionTime": "2023-07-28T20:10:14.631744Z",
                },
            ),
        ],
    )
    def test_get_ready_condition(self, terminal_condition, expected_value):
        """
        tests that the get ready condition function returns the correct ready condition,
        if applicable
        """
        job = JobV2(
            name="test-name",
            uid="123-uuid",
            generation="1",
            create_time="create-time",
            update_time="update-time",
            launch_stage="launch-stage",
            template={},
            terminal_condition=terminal_condition,
            latest_created_execution={},
            etag="etag-123",
        )

        assert job.get_ready_condition() == expected_value

    @pytest.mark.parametrize(
        "latest_created_execution,expected_value",
        [
            ({}, {}),
            (
                {
                    "createTime": "1970-01-01T00:00:00Z",
                    "completionTime": "1970-01-01T00:00:00Z",
                },
                {
                    "createTime": "1970-01-01T00:00:00Z",
                    "completionTime": "1970-01-01T00:00:00Z",
                },
            ),
        ],
    )
    def test_get_execution_status(self, latest_created_execution, expected_value):
        """
        tests whether get execution function returns the correct response
        """
        job = JobV2(
            name="test-name",
            uid="123-uuid",
            generation="1",
            create_time="create-time",
            update_time="update-time",
            launch_stage="launch-stage",
            template={},
            terminal_condition={},
            latest_created_execution=latest_created_execution,
            etag="etag-123",
        )

        assert job._get_execution_status() == expected_value

    @pytest.mark.parametrize(
        "latest_created_execution,expected_value",
        [
            (
                {"createTime": "1970-01-01T00:00:00Z"},
                True,
            ),
            (
                {
                    "createTime": "1970-01-01T00:00:00Z",
                    "completionTime": "1970-01-01T00:00:00Z",
                },
                True,
            ),
            (
                {
                    "createTime": "1970-01-01T00:00:00Z",
                    "completionTime": "2023-01-01T00:12:34Z",
                },
                False,
            ),
        ],
    )
    def test_has_execution_in_progress(
        self,
        latest_created_execution,
        expected_value,
    ):
        """
        tests that has_execution_in_progress works correctly when given default
        timestamp, no timestamp, and accurate/finished timestamp
        """
        job = JobV2(
            name="test-name",
            uid="123-uuid",
            generation="1",
            create_time="create-time",
            update_time="update-time",
            launch_stage="launch-stage",
            template={},
            terminal_condition={},
            latest_created_execution=latest_created_execution,
            etag="etag-123",
        )

        assert job.has_execution_in_progress() == expected_value

    def test_get_calls_correct_methods(self, mock_client):
        """
        tests that calls are called with correct job name, project, and location
        """
        mock_client.jobs().get().execute.return_value = jobs_return_value

        JobV2.get(
            client=mock_client,
            project="project-123",
            location="location-123",
            job_name="test-job-name",
        )

        desired_calls = [
            "call.jobs().get()",
            "call.jobs().get(name='projects/project-123/locations/location-123/jobs/"
            "test-job-name')",
            "call.jobs().get().execute()",
        ]

        actual_calls = list_mock_calls(mock_client=mock_client)

        assert actual_calls == desired_calls


@pytest.fixture
def cloud_run_job_v2(service_account_info):
    return CloudRunJobV2(
        image="gcr.io//hello/there-image",
        region="skylords-house",
        credentials=GcpCredentials(service_account_info=service_account_info),
    )


def remove_server_url_from_env(env):
    """
    For convenience since the testing database URL is non-deterministic.
    """
    return [
        env_var
        for env_var in env
        if env_var["name"]
        not in [
            "PREFECT_API_DATABASE_CONNECTION_URL",
            "PREFECT_ORION_DATABASE_CONNECTION_URL",
            "PREFECT_SERVER_DATABASE_CONNECTION_URL",
        ]
    ]


class TestCloudRunJobV2ContainerSettings:
    def test_captures_prefect_env(self, cloud_run_job_v2):
        base_setting = {}
        with temporary_settings(
            updates={
                PREFECT_API_KEY: "Dog",
                PREFECT_API_URL: "Puppy",
                PREFECT_PROFILES_PATH: "Woof",
            }
        ):
            result = cloud_run_job_v2._add_container_settings(base_setting)
            assert remove_server_url_from_env(result["env"]) == [
                {"name": "PREFECT_API_URL", "value": "Puppy"},
                {"name": "PREFECT_API_KEY", "value": "Dog"},
                {"name": "PREFECT_PROFILES_PATH", "value": "Woof"},
            ]

    def test_adds_job_env(self, cloud_run_job_v2):
        base_setting = {}
        cloud_run_job_v2.env = {"TestVar": "It's Working"}

        with temporary_settings(
            updates={
                PREFECT_API_KEY: "Dog",
                PREFECT_API_URL: "Puppy",
                PREFECT_PROFILES_PATH: "Woof",
            }
        ):
            result = cloud_run_job_v2._add_container_settings(base_setting)
            assert remove_server_url_from_env(result["env"]) == [
                {"name": "PREFECT_API_URL", "value": "Puppy"},
                {"name": "PREFECT_API_KEY", "value": "Dog"},
                {"name": "PREFECT_PROFILES_PATH", "value": "Woof"},
                {"name": "TestVar", "value": "It's Working"},
            ]

    def test_command_overrides_default(self, cloud_run_job_v2):
        cmd = ["echo", "howdy!"]
        cloud_run_job_v2.command = cmd
        base_setting = {}
        result = cloud_run_job_v2._add_container_settings(base_setting)
        assert result["command"] == cmd

    def test_resources_skipped_by_default(self, cloud_run_job_v2):
        base_setting = {}
        result = cloud_run_job_v2._add_container_settings(base_setting)
        assert result.get("resources") is None

    def test_resources_added_correctly(self, cloud_run_job_v2):
        cpu = 1
        memory = 12
        memory_unit = "G"
        cloud_run_job_v2.cpu = cpu
        cloud_run_job_v2.memory = memory
        cloud_run_job_v2.memory_unit = memory_unit
        base_setting = {}
        result = cloud_run_job_v2._add_container_settings(base_setting)
        expected_cpu = "1000m"
        assert result["resources"] == {
            "limits": {"cpu": expected_cpu, "memory": str(memory) + memory_unit},
        }

    def test_timeout_added_correctly(self, cloud_run_job_v2):
        timeout = 10
        cloud_run_job_v2.timeout = timeout
        result = cloud_run_job_v2._jobs_body()
        assert result["template"]["template"]["timeout"] == f"{timeout}s"

    def test_vpc_connector_name_added_correctly(self, cloud_run_job_v2):
        cloud_run_job_v2.vpc_connector_name = "vpc_name"
        result = cloud_run_job_v2._jobs_body()
        assert result["template"]["template"]["vpcAccess"]["connector"] == "vpc_name"

    def test_max_retries_added_correctly(self, cloud_run_job_v2):
        cloud_run_job_v2.max_retries = 5
        result = cloud_run_job_v2._jobs_body()
        assert result["template"]["template"]["maxRetries"] == 5

    def test_memory_validation_succeeds(self, gcp_credentials):
        """
        Make sure that memory validation doesn't fail when valid params provided.
        """
        CloudRunJobV2(
            image="gcr.io//hello/there-image",
            region="skylords-house",
            credentials=gcp_credentials,
            cpu=1,
            memory=1,
            memory_unit="G",
        )

    def test_memory_validation_fails(self, gcp_credentials):
        """
        Make sure memory validation fails without both unit and memory
        """
        with pytest.raises(pydantic.error_wrappers.ValidationError):
            CloudRunJobV2(
                image="gcr.io//hello/there-image",
                region="skylords-house",
                credentials=gcp_credentials,
                cpu=1,
                memory_unit="G",
            )
        with pytest.raises(pydantic.error_wrappers.ValidationError):
            CloudRunJobV2(
                image="gcr.io//hello/there-image",
                region="skylords-house",
                credentials=gcp_credentials,
                cpu=1,
                memory=1,
            )

    def test_args_skipped_by_default(self, cloud_run_job_v2):
        base_setting = {}

        result = cloud_run_job_v2._add_container_settings(base_setting)

        assert result.get("args") is None

    def test_args_added_correctly(self, cloud_run_job_v2):
        args = ["a", "b"]

        cloud_run_job_v2.args = args

        base_setting = {}

        result = cloud_run_job_v2._add_container_settings(base_setting)

        assert result["args"] == args


def test_get_client_uses_correct_endpoint(
    monkeypatch,
    mock_credentials,
    cloud_run_job_v2,
):
    """
    Expected behavior: desired endpoint is called.
    """
    mock = Mock()

    monkeypatch.setattr("prefect_gcp.cloud_run.discovery.build", mock)

    cloud_run_job_v2._get_client()

    desired_endpoint = f"https://{cloud_run_job_v2.region}-run.googleapis.com"

    assert mock.call_args[1]["client_options"].api_endpoint == desired_endpoint


class TestCloudRunJobRunV2:
    job_ready = {
        "name": "test-job-name",
        "uid": "2bf3b61d",
        "generation": "1",
        "createTime": "2023-07-28T21:44:41.856516Z",
        "updateTime": "2023-07-28T21:44:41.856516Z",
        "launchStage": "BETA",
        "template": {
            "parallelism": 1,
            "taskCount": 1,
            "template": {
                "containers": [
                    {
                        "image": "gcr.io//hello/there-image",
                        "env": [
                            {
                                "name": "PREFECT_API_URL",
                                "value": "super-secret-url",
                            },
                            {
                                "name": "PREFECT_API_KEY",
                                "value": "super-strong-api-key",
                            },
                        ],
                        "resources": {"limits": {"cpu": "1000m", "memory": "512Mi"}},
                    }
                ],
                "maxRetries": 1,
                "timeout": "600s",
            },
        },
        "terminalCondition": {
            "type": "Ready",
            "state": "CONDITION_SUCCEEDED",
            "lastTransitionTime": "2023-07-28T20:10:14.631744Z",
        },
        "latestCreatedExecution": {
            "createTime": "1970-01-01T00:00:00Z",
            "completionTime": "1970-01-01T00:00:00Z",
        },
        "reconciling": True,
        "etag": '"etag-123"',
    }
    job_running = {
        "name": "operations-name",
        "metadata": {
            "@type": "type.googleapis.com/google.cloud.run.v2.Execution",
            "name": "projects/project-123/locations/skylords-house/jobs/prefect-flows-"
            "84333d85288e4621b7b2e3c6f6896393/executions/prefect-flows-"
            "15e2a1517ea049d89c49e7f6c5b09230-hr69d",
            "uid": "2bf3b61d",
            "generation": "1",
            "createTime": "2023-07-28T21:44:41.856516Z",
            "updateTime": "2023-07-28T21:44:41.856516Z",
            "launchStage": "BETA",
            "job": "prefect-flows-84333d85288e4621b7b2e3c6f6896393",
            "parallelism": 1,
            "taskCount": 1,
            "template": {
                "containers": [
                    {
                        "image": "gcr.io//hello/there-image",
                        "env": [
                            {
                                "name": "PREFECT_API_URL",
                                "value": "super-secret-url",
                            },
                            {
                                "name": "PREFECT_API_KEY",
                                "value": "super-strong-api-key",
                            },
                        ],
                        "resources": {"limits": {"cpu": "1000m", "memory": "512Mi"}},
                    }
                ],
                "maxRetries": 1,
                "timeout": "600s",
            },
            "reconciling": True,
            "conditions": [{"type": "Completed", "state": "CONDITION_PENDING"}],
            "log_uri": "www.logs.com",
            "etag": '"etag-123"',
        },
    }
    execution_complete_and_succeeded = {
        "name": "test-job-name",
        "uid": "2bf3b61d",
        "generation": "1",
        "labels": None,
        "annotations": None,
        "createTime": "2023-07-28T21:44:41.856516Z",
        "startTime": "2023-07-28T21:56:27.607948Z",
        "completionTime": "2023-07-28T21:56:43.798685Z",
        "updateTime": "2023-07-28T21:44:41.856516Z",
        "deleteTime": None,
        "expireTime": None,
        "launchStage": "BETA",
        "job": "prefect-flows-84333d85288e4621b7b2e3c6f6896393",
        "parallelism": 1,
        "taskCount": 1,
        "template": {
            "containers": [
                {
                    "image": "gcr.io//hello/there-image",
                    "env": [
                        {
                            "name": "PREFECT_API_URL",
                            "value": "super-secret-url",
                        },
                        {
                            "name": "PREFECT_API_KEY",
                            "value": "super-strong-api-key",
                        },
                    ],
                    "resources": {"limits": {"cpu": "1000m", "memory": "512Mi"}},
                }
            ],
            "maxRetries": 1,
            "timeout": "600s",
        },
        "reconciling": None,
        "conditions": [
            {
                "type": "ResourcesAvailable",
                "state": "CONDITION_SUCCEEDED",
                "lastTransitionTime": "2023-07-28T21:56:32.778835Z",
            },
            {
                "type": "Started",
                "state": "CONDITION_SUCCEEDED",
                "lastTransitionTime": "2023-07-28T21:56:32.910737Z",
            },
            {
                "type": "Completed",
                "state": "CONDITION_SUCCEEDED",
                "lastTransitionTime": "2023-07-28T21:56:44.311464Z",
            },
        ],
        "observed_generation": "1",
        "succeeded_count": 1,
        "logUri": "www.logs.com",
        "etag": '"etag-123"',
    }

    @pytest.mark.parametrize(
        "keep_job,expected_calls",
        [
            (
                True,
                [
                    "call.jobs().create",
                    "call.jobs().create().execute()",
                ],
            ),
            (
                False,
                [
                    "call.jobs().create",
                    "call.jobs().create().execute()",
                    "call.jobs().get",
                    "call.jobs().get().execute()",
                    "call.jobs().run",
                    "call.jobs().run().execute()",
                ],
            ),
        ],
    )
    def test_happy_path_api_calls_made_correctly(
        self,
        mock_client,
        cloud_run_job_v2,
        keep_job,
        expected_calls,
    ):
        """
        Expected behavior:
        Happy path:
        - A call to Job.create and execute
        - A call to Job.get and execute (check status)
        - A call to Job.run and execute (start the job when status is ready)
        - A call to Executions.get and execute (see the status of the Execution)
        - A call to Job.delete and execute if `keep_job` False, otherwise no call
        """
        cloud_run_job_v2.keep_job = keep_job
        mock_client.jobs().get().execute.return_value = self.job_ready
        mock_client.jobs().run().execute.return_value = self.job_running
        mock_client.jobs().executions().get().execute.return_value = (
            self.execution_complete_and_succeeded
        )
        cloud_run_job_v2.run()
        calls = list_mock_calls(mock_client, 4)

        for call, expected_call in zip(calls, expected_calls):
            assert call.startswith(expected_call)

    def test_happy_path_result(self, mock_client, cloud_run_job_v2):
        """
        Expected behavior: returns a CloudrunJobResult with status_code 0
        """
        cloud_run_job_v2.keep_job = True
        mock_client.jobs().get().execute.return_value = self.job_ready
        mock_client.jobs().run().execute.return_value = self.job_running
        mock_client.jobs().executions().get().execute.return_value = (
            self.execution_complete_and_succeeded
        )
        res = cloud_run_job_v2.run()

        assert isinstance(res, CloudRunJobResultV2)
        assert res.status_code == 0

    @pytest.mark.parametrize(
        "keep_job,expected_calls",
        [
            (
                True,
                [
                    "call.jobs().create",
                    "call.jobs().create().execute()",
                ],
            ),
            (
                False,
                [
                    "call.jobs().create",
                    "call.jobs().create().execute()",
                    "call.jobs().delete",
                    "call.jobs().delete().execute()",
                ],
            ),
        ],
    )
    def test_behavior_called_when_job_get_fails(
        self,
        monkeypatch,
        mock_client,
        cloud_run_job_v2,
        keep_job,
        expected_calls,
    ):
        """
        Expected behavior:
        When job create is called, but there is a subsequent exception on a get,
        there should be a delete call for that job if `keep_job` is False
        """
        cloud_run_job_v2.keep_job = keep_job

        # Getting job will raise error
        def raise_exception(*args, **kwargs):
            raise Exception("This is an intentional exception")

        monkeypatch.setattr("prefect_gcp.cloud_run_v2.JobV2.get", raise_exception)

        with pytest.raises(Exception):
            cloud_run_job_v2.run()
        calls = list_mock_calls(mock_client, 0)

        for call, expected_call in zip(calls, expected_calls):
            assert call.startswith(expected_call)

    # Test that RuntimeError raised if something happens with execution
    @pytest.mark.parametrize(
        "keep_job,expected_calls",
        [
            (
                True,
                [
                    "call.jobs().create",
                    "call.jobs().create().execute()",
                    "call.jobs().get",
                    "call.jobs().get().execute()",
                    "call.jobs().run",
                    "call.jobs().run().execute()",
                ],
            ),
            (
                False,
                [
                    "call.jobs().create",
                    "call.jobs().create().execute()",
                    "call.jobs().get",
                    "call.jobs().get().execute()",
                    "call.jobs().run",
                    "call.jobs().run().execute()",
                    "call.jobs().delete",
                    "call.jobs().delete().execute()",
                ],
            ),
        ],
    )
    def test_behavior_called_when_execution_get_fails(
        self,
        monkeypatch,
        mock_client,
        cloud_run_job_v2,
        keep_job,
        expected_calls,
    ):
        """
        Expected behavior:
        Job creation is called successfully, the job is found when `get` is called,
        job `run` is called, but the execution fails, there should be a delete
        call for that job if `keep_job` is False, and an exception should be raised.
        """
        cloud_run_job_v2.keep_job = keep_job
        mock_client.jobs().get().execute.return_value = self.job_ready
        mock_client.jobs().run().execute.return_value = self.job_ready

        # Getting execution will raise error
        def raise_exception(*args, **kwargs):
            raise Exception("This is an intentional exception")

        monkeypatch.setattr("prefect_gcp.cloud_run.Execution.get", raise_exception)

        with pytest.raises(Exception):
            cloud_run_job_v2.run()
        calls = list_mock_calls(mock_client, 2)
        for call, expected_call in zip(calls, expected_calls):
            assert call.startswith(expected_call)
