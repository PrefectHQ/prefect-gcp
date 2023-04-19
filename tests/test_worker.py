from unittest.mock import Mock

import pytest
from prefect.client.schemas import FlowRun
from pydantic import ValidationError

from prefect_gcp.credentials import GcpCredentials
from prefect_gcp.worker import (
    CloudRunWorker,
    CloudRunWorkerJobConfiguration,
    CloudRunWorkerResult,
    CloudRunWorkerVariables,
)


@pytest.fixture(autouse=True)
def mock_credentials(gcp_credentials):
    yield


@pytest.fixture
def jobs_body():
    return {
        "apiVersion": "run.googleapis.com/v1",
        "kind": "Job",
        "metadata": {
            "annotations": {
                # See: https://cloud.google.com/run/docs/troubleshooting#launch-stage-validation  # noqa
                "run.googleapis.com/launch-stage": "BETA",
            },
        },
        "spec": {  # JobSpec
            "template": {  # ExecutionTemplateSpec
                "spec": {  # ExecutionSpec
                    "template": {  # TaskTemplateSpec
                        "spec": {"containers": [{}]},  # TaskSpec
                    },
                },
            },
        },
    }


@pytest.fixture
def flow_run():
    return FlowRun(name="my-flow-run-name")


@pytest.fixture
def cloud_run_worker_job_config(service_account_info, jobs_body, flow_run):
    return CloudRunWorkerJobConfiguration(
        image="gcr.io//not-a/real-image",
        region="middle-earth2",
        job_body=jobs_body,
        credentials=GcpCredentials(service_account_info=service_account_info),
    )


@pytest.fixture
def cloud_run_worker():
    return CloudRunWorker("my-work-pool")


class TestCloudRunWorkerJobConfiguration:
    def test_project_property(self, service_account_info, cloud_run_worker_job_config):
        assert cloud_run_worker_job_config.project == service_account_info["project_id"]

    def test_job_name(self, cloud_run_worker_job_config):
        cloud_run_worker_job_config.job_body["metadata"]["name"] = "my-job-name"
        assert cloud_run_worker_job_config.job_name == "my-job-name"

    def test_populate_envs(
        self,
        cloud_run_worker_job_config,
    ):
        container = cloud_run_worker_job_config.job_body["spec"]["template"]["spec"][
            "template"
        ]["spec"]["containers"][0]

        assert "env" not in container
        assert cloud_run_worker_job_config.env == {}
        cloud_run_worker_job_config.env = {"a": "b"}

        cloud_run_worker_job_config._populate_envs()

        assert "env" in container
        assert len(container["env"]) == 1
        assert container["env"][0]["name"] == "a"
        assert container["env"][0]["value"] == "b"

    def test_populate_image_if_not_present(self, cloud_run_worker_job_config):
        container = cloud_run_worker_job_config.job_body["spec"]["template"]["spec"][
            "template"
        ]["spec"]["containers"][0]

        assert "image" not in container

        cloud_run_worker_job_config._populate_image_if_not_present()

        # defaults to prefect image
        assert "image" in container
        assert container["image"].startswith("docker.io/prefecthq/prefect:")

    def test_populate_image_doesnt_overwrite(self, cloud_run_worker_job_config):
        image = "my-first-image"
        container = cloud_run_worker_job_config.job_body["spec"]["template"]["spec"][
            "template"
        ]["spec"]["containers"][0]
        container["image"] = image

        cloud_run_worker_job_config._populate_image_if_not_present()

        assert container["image"] == image

    def test_populate_args_if_not_present(self, cloud_run_worker_job_config):
        container = cloud_run_worker_job_config.job_body["spec"]["template"]["spec"][
            "template"
        ]["spec"]["containers"][0]

        assert "args" not in container

        cloud_run_worker_job_config._populate_command_if_not_present()

        assert "args" in container
        assert container["args"] == ["python", "-m", "prefect.engine"]

    def test_populate_name_if_not_present(self, cloud_run_worker_job_config, flow_run):
        metadata = cloud_run_worker_job_config.job_body["metadata"]

        assert "name" not in metadata

        cloud_run_worker_job_config._populate_name_if_not_present(flow_run)

        assert "name" in metadata
        assert metadata["name"] == flow_run.name

    def test_populate_name_doesnt_overwrite(
        self, cloud_run_worker_job_config, flow_run
    ):
        name = "my-name"
        metadata = cloud_run_worker_job_config.job_body["metadata"]
        metadata["name"] = name

        cloud_run_worker_job_config._populate_name_if_not_present(flow_run)

        assert "name" in metadata
        assert metadata["name"] != flow_run.name
        assert metadata["name"] == name

    async def test_validates_against_an_empty_job_body(self):
        template = CloudRunWorker.get_default_base_job_template()
        template["job_configuration"]["job_body"] = {}
        template["job_configuration"]["region"] = "test-region1"

        with pytest.raises(ValidationError) as excinfo:
            await CloudRunWorkerJobConfiguration.from_template_and_values(template, {})

        assert excinfo.value.errors() == [
            {
                "loc": ("job_body",),
                "msg": (
                    "Job is missing required attributes at the following paths: "
                    "/apiVersion, /kind, /metadata, /spec"
                ),
                "type": "value_error",
            }
        ]

    async def test_validates_for_a_job_body_missing_deeper_attributes(self):
        template = CloudRunWorker.get_default_base_job_template()
        template["job_configuration"]["job_body"] = {
            "apiVersion": "run.googleapis.com/v1",
            "kind": "Job",
            "metadata": {},
            "spec": {"template": {"spec": {"template": {"spec": {}}}}},
        }
        template["job_configuration"]["region"] = "test-region1"

        with pytest.raises(ValidationError) as excinfo:
            await CloudRunWorkerJobConfiguration.from_template_and_values(template, {})

        assert excinfo.value.errors() == [
            {
                "loc": ("job_body",),
                "msg": (
                    "Job is missing required attributes at the following paths: "
                    "/metadata/annotations, "
                    "/spec/template/spec/template/spec/containers"
                ),
                "type": "value_error",
            }
        ]

    async def test_validates_for_a_job_with_incompatible_values(self):
        """We should give a human-friendly error when the user provides a custom Job
        manifest that is attempting to change required values."""
        template = CloudRunWorker.get_default_base_job_template()
        template["job_configuration"]["region"] = "test-region1"
        template["job_configuration"]["job_body"] = {
            "apiVersion": "v1",
            "kind": "NOTAJOB",
            "metadata": {
                "annotations": {
                    "run.googleapis.com/launch-stage": "NOTABETA",
                },
            },
            "spec": {  # JobSpec
                "template": {  # ExecutionTemplateSpec
                    "spec": {  # ExecutionSpec
                        "template": {  # TaskTemplateSpec
                            "spec": {"containers": [{}]},  # TaskSpec
                        },
                    },
                },
            },
        }

        with pytest.raises(ValidationError) as excinfo:
            await CloudRunWorkerJobConfiguration.from_template_and_values(template, {})

        assert excinfo.value.errors() == [
            {
                "loc": ("job_body",),
                "msg": (
                    "Job has incompatible values for the following attributes: "
                    "/apiVersion must have value 'run.googleapis.com/v1', "
                    "/kind must have value 'Job', "
                    "/metadata/annotations/run.googleapis.com~1launch-stage must have value 'BETA'"
                ),
                "type": "value_error",
            }
        ]


class TestCloudRunWorkerVariables:
    def test_region_is_required(self):
        with pytest.raises(ValidationError) as excinfo:
            CloudRunWorkerVariables()
        assert excinfo.value.errors() == [
            {"loc": ("region",), "msg": "field required", "type": "value_error.missing"}
        ]

    def test_valid_cpu_string(self):
        with pytest.raises(ValidationError):
            CloudRunWorkerVariables(region="test-region1", cpu="1")
        with pytest.raises(ValidationError):
            CloudRunWorkerVariables(region="test-region1", cpu="100")
        with pytest.raises(ValidationError):
            CloudRunWorkerVariables(region="test-region1", cpu="100m")
        with pytest.raises(ValidationError):
            CloudRunWorkerVariables(region="test-region1", cpu="1500m")

        CloudRunWorkerVariables(region="test-region1", cpu="1000m")
        CloudRunWorkerVariables(region="test-region1", cpu="2000m")
        CloudRunWorkerVariables(region="test-region1", cpu="3000m")

    def test_valid_memory_string(self):
        with pytest.raises(ValidationError):
            CloudRunWorkerVariables(region="test-region1", memory="1")
        with pytest.raises(ValidationError):
            CloudRunWorkerVariables(region="test-region1", memory="100")
        with pytest.raises(ValidationError):
            CloudRunWorkerVariables(region="test-region1", memory="100Gigs")

        CloudRunWorkerVariables(region="test-region1", memory="512G")
        CloudRunWorkerVariables(region="test-region1", memory="512Gi")
        CloudRunWorkerVariables(region="test-region1", memory="512M")
        CloudRunWorkerVariables(region="test-region1", memory="512Mi")


executions_return_value = {
    "metadata": {"name": "test-name", "namespace": "test-namespace"},
    "spec": {"MySpec": "spec"},
    "status": {"logUri": "test-log-uri"},
}

jobs_return_value = {
    "metadata": {"name": "Test", "namespace": "test-namespace"},
    "spec": {"MySpec": "spec"},
    "status": {
        "conditions": [{"type": "Ready", "dog": "cat"}],
        "latestCreatedExecution": {"puppy": "kitty"},
    },
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
        "prefect_gcp.worker.CloudRunWorker._get_client",
        get_mock_client,
    )

    return m


class MockExecution(Mock):
    call_count = 0

    def __init__(self, succeeded=False, *args, **kwargs):
        super().__init__()
        self.log_uri = "test_uri"
        self._succeeded = succeeded

    def is_running(self):
        MockExecution.call_count += 1

        if self.call_count > 2:
            return False
        return True

    def condition_after_completion(self):
        return {"message": "test"}

    def succeeded(self):
        return self._succeeded

    @classmethod
    def get(cls, *args, **kwargs):
        return cls()


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


class TestCloudRunWorker:
    job_ready = {
        "metadata": {"name": "Test", "namespace": "test-namespace"},
        "spec": {"MySpec": "spec"},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "latestCreatedExecution": {"puppy": "kitty"},
        },
    }
    execution_ready = {
        "metadata": {"name": "test-name", "namespace": "test-namespace"},
        "spec": {"MySpec": "spec"},
        "status": {"logUri": "test-log-uri"},
    }
    execution_complete_and_succeeded = {
        "metadata": {"name": "test-name", "namespace": "test-namespace"},
        "spec": {"MySpec": "spec"},
        "status": {
            "logUri": "test-log-uri",
            "completionTime": "Done!",
            "conditions": [{"type": "Completed", "status": "True"}],
        },
    }
    execution_not_found = {
        "metadata": {"name": "test-name", "namespace": "test-namespace"},
        "spec": {"MySpec": "spec"},
        "status": {
            "logUri": "test-log-uri",
            "completionTime": "Done!",
            "conditions": [
                {"type": "Completed", "status": "False", "message": "Not found"}
            ],
        },
    }
    execution_complete_and_failed = {
        "metadata": {"name": "test-name", "namespace": "test-namespace"},
        "spec": {"MySpec": "spec"},
        "status": {
            "logUri": "test-log-uri",
            "completionTime": "Done!",
            "conditions": [
                {"type": "Completed", "status": "False", "message": "test failure"}
            ],
        },
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
        cloud_run_worker,
        cloud_run_worker_job_config,
        keep_job,
        flow_run,
        expected_calls,
    ):
        """Expected behavior:
        Happy path:
        - A call to Job.create and execute
        - A call to Job.get and execute (check status)
        - A call to Job.run and execute (start the job when status is ready)
        - A call to Executions.get and execute (see the status of the Execution)
        - A call to Job.delete and execute if `keep_job` False, otherwise no call
        """
        cloud_run_worker_job_config.keep_job = keep_job
        cloud_run_worker_job_config.prepare_for_flow_run(flow_run, None, None)
        mock_client.jobs().get().execute.return_value = self.job_ready
        mock_client.jobs().run().execute.return_value = self.job_ready
        mock_client.executions().get().execute.return_value = (
            self.execution_complete_and_succeeded
        )
        cloud_run_worker.run(flow_run, cloud_run_worker_job_config)
        calls = list_mock_calls(mock_client, 3)

        for call, expected_call in zip(calls, expected_calls):
            assert call.startswith(expected_call)

    def test_happy_path_result(
        self, mock_client, cloud_run_worker, cloud_run_worker_job_config, flow_run
    ):
        """Expected behavior: returns a CloudrunJobResult with status_code 0"""
        cloud_run_worker_job_config.keep_job = True
        cloud_run_worker_job_config.prepare_for_flow_run(flow_run, None, None)
        mock_client.jobs().get().execute.return_value = self.job_ready
        mock_client.jobs().run().execute.return_value = self.job_ready
        mock_client.executions().get().execute.return_value = (
            self.execution_complete_and_succeeded
        )
        res = cloud_run_worker.run(flow_run, cloud_run_worker_job_config)
        assert isinstance(res, CloudRunWorkerResult)
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
        cloud_run_worker,
        cloud_run_worker_job_config,
        flow_run,
        keep_job,
        expected_calls,
    ):
        """Expected behavior:
        When job create is called, but there is a subsequent exception on a get,
        there should be a delete call for that job if `keep_job` is False
        """
        cloud_run_worker_job_config.keep_job = keep_job

        # Getting job will raise error
        def raise_exception(*args, **kwargs):
            raise Exception("This is an intentional exception")

        monkeypatch.setattr("prefect_gcp.cloud_run.Job.get", raise_exception)

        with pytest.raises(Exception):
            cloud_run_worker.run(flow_run, cloud_run_worker_job_config)
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
        cloud_run_worker,
        cloud_run_worker_job_config,
        flow_run,
        keep_job,
        expected_calls,
    ):
        """Expected behavior:
        Job creation is called successfully, the job is found when `get` is called,
        job `run` is called, but the execution fails, there should be a delete
        call for that job if `keep_job` is False, and an exception should be raised.
        """
        cloud_run_worker_job_config.keep_job = keep_job
        mock_client.jobs().get().execute.return_value = self.job_ready
        mock_client.jobs().run().execute.return_value = self.job_ready

        # Getting execution will raise error
        def raise_exception(*args, **kwargs):
            raise Exception("This is an intentional exception")

        monkeypatch.setattr("prefect_gcp.cloud_run.Execution.get", raise_exception)

        with pytest.raises(Exception):
            cloud_run_worker.run(flow_run, cloud_run_worker_job_config)
        calls = list_mock_calls(mock_client, 2)
        # breakpoint()
        for call, expected_call in zip(calls, expected_calls):
            assert call.startswith(expected_call)
