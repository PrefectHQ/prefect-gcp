from unittest.mock import Mock

import pytest

from prefect_gcp.cloud_run_v2 import CloudRunJobV2, CloudRunJobV2Result, JobV2
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
    "launchStage": "BETA",
    "binaryAuthorization": {},
    "template": {},
    "observedGeneration": "1",
    "terminalCondition": {},
    "conditions": [],
    "executionCount": 1,
    "latestCreatedExecution": {},
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
        "state,expected",
        [("CONDITION_SUCCEEDED", True), ("CONDITION_FAILED", False)],
    )
    def test_is_ready(self, state, expected):
        job = JobV2(
            name="test-job",
            uid="12345",
            generation="2",
            labels={},
            annotations={},
            createTime="2021-08-31T18:00:00Z",
            updateTime="2021-08-31T18:00:00Z",
            launchStage="BETA",
            binaryAuthorization={},
            template={},
            terminalCondition={
                "type": "Ready",
                "state": state,
            },
            conditions=[],
            executionCount=1,
            latestCreatedExecution={},
            reconciling=False,
            satisfiesPzs=False,
            etag="etag-12345",
        )

        assert job.is_ready() == expected

    def test_is_ready_raises_exception(self):
        job = JobV2(
            name="test-job",
            uid="12345",
            generation="2",
            labels={},
            annotations={},
            createTime="2021-08-31T18:00:00Z",
            updateTime="2021-08-31T18:00:00Z",
            launchStage="BETA",
            binaryAuthorization={},
            template={},
            terminalCondition={
                "type": "Ready",
                "state": "CONTAINER_FAILED",
                "reason": "ContainerMissing",
            },
            conditions=[],
            executionCount=1,
            latestCreatedExecution={},
            reconciling=False,
            satisfiesPzs=False,
            etag="etag-12345",
        )

        with pytest.raises(Exception):
            job.is_ready()

    @pytest.mark.parametrize(
        "terminal_condition,expected",
        [
            (
                {
                    "type": "Ready",
                    "state": "CONDITION_SUCCEEDED",
                },
                {
                    "type": "Ready",
                    "state": "CONDITION_SUCCEEDED",
                },
            ),
            (
                {
                    "type": "Failed",
                    "state": "CONDITION_FAILED",
                },
                {},
            ),
        ],
    )
    def test_get_ready_condition(self, terminal_condition, expected):
        job = JobV2(
            name="test-job",
            uid="12345",
            generation="2",
            labels={},
            annotations={},
            createTime="2021-08-31T18:00:00Z",
            updateTime="2021-08-31T18:00:00Z",
            launchStage="BETA",
            binaryAuthorization={},
            template={},
            terminalCondition=terminal_condition,
            conditions=[],
            executionCount=1,
            latestCreatedExecution={},
            reconciling=False,
            satisfiesPzs=False,
            etag="etag-12345",
        )

        assert job.get_ready_condition() == expected

    @pytest.mark.parametrize(
        "ready_condition,expected",
        [
            (
                {
                    "state": "CONTAINER_FAILED",
                    "reason": "ContainerMissing",
                },
                True,
            ),
            (
                {
                    "state": "CONDITION_SUCCEEDED",
                },
                False,
            ),
        ],
    )
    def test_is_missing_container(self, ready_condition, expected):
        job = JobV2(
            name="test-job",
            uid="12345",
            generation="2",
            labels={},
            annotations={},
            createTime="2021-08-31T18:00:00Z",
            updateTime="2021-08-31T18:00:00Z",
            launchStage="BETA",
            binaryAuthorization={},
            template={},
            terminalCondition={},
            conditions=[],
            executionCount=1,
            latestCreatedExecution={},
            reconciling=False,
            satisfiesPzs=False,
            etag="etag-12345",
        )

        assert job._is_missing_container(ready_condition=ready_condition) == expected

    def test_get_calls_correct_methods(self, mock_client):
        mock_client.jobs().get().execute.return_value = jobs_return_value

        JobV2.get(
            cr_client=mock_client,
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

    desired_endpoint = "https://run.googleapis.com"

    assert mock.call_args[1]["client_options"].api_endpoint == desired_endpoint


class TestCloudRunJobRunV2:
    job_ready = {
        "name": "test-job-name",
        "uid": "2bf3b61d",
        "generation": "1",
        "labels": {},
        "annotations": {},
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
        "labels": {},
        "annotations": {},
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
        "reconciling": True,
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

        assert isinstance(res, CloudRunJobV2Result)
        assert res.status_code == 0

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
