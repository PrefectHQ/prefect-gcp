import uuid
from unittest.mock import MagicMock

import pytest
from google.cloud.aiplatform_v1.types.job_state import JobState
from prefect.client.schemas import FlowRun

from prefect_gcp.vertex_worker import (
    VertexAIWorker,
    VertexAIWorkerJobConfiguration,
    VertexAIWorkerResult,
)


@pytest.fixture
def job_config(service_account_info, gcp_credentials):
    return VertexAIWorkerJobConfiguration(
        name="my-custom-ai-job",
        image="gcr.io/your-project/your-repo:latest",
        region="ashenvale",
        credentials=gcp_credentials,
        boot_disk_size_gb=100,
    )


@pytest.fixture
def flow_run():
    return FlowRun(flow_id=uuid.uuid4(), name="my-flow-run-name")


class TestVertexAIWorkerJobConfiguration:
    def test_gcp_project(self, job_config: VertexAIWorkerJobConfiguration):
        assert job_config.project == "gcp_credentials_project"

    def test_job_name(self, job_config: VertexAIWorkerJobConfiguration):
        assert job_config.job_name.startswith("your-repo:latest")

        with pytest.raises(
            ValueError,
            match="The provided image must be from either Google Container Registry",
        ):
            job_config.image = "prefect/image:latest"
            job_config.job_name  # should raise ValueError

    def test_command_as_list(self, job_config: VertexAIWorkerJobConfiguration):
        assert [
            "python",
            "-m",
            "prefect.engine",
        ] == job_config.command_as_list()

        job_config.command = "echo -n hello"
        assert ["echo", "-n", "hello"] == job_config.command_as_list()


class TestVertexAIWorker:
    async def test_successful_worker_run(self, flow_run, job_config):
        async with VertexAIWorker("test-pool") as worker:
            result = await worker.run(flow_run=flow_run, configuration=job_config)
            assert (
                job_config.credentials.job_service_client.create_custom_job.call_count
                == 1
            )
            assert (
                job_config.credentials.job_service_client.get_custom_job.call_count == 1
            )
            assert result == VertexAIWorkerResult(
                status_code=0, identifier="mock_display_name"
            )

    async def test_failed_worker_run(self, flow_run, job_config):
        error_msg = "something went kablooey"
        error_job_display_name = "catastrophization"
        job_config.credentials.job_service_client.get_custom_job.return_value = (
            MagicMock(
                name="error_mock_name",
                state=JobState.JOB_STATE_FAILED,
                error=MagicMock(message=error_msg),
                display_name=error_job_display_name,
            )
        )
        async with VertexAIWorker("test-pool") as worker:
            with pytest.raises(RuntimeError, match=error_msg):
                await worker.run(flow_run=flow_run, configuration=job_config)

            assert (
                job_config.credentials.job_service_client.create_custom_job.call_count
                == 1
            )
            assert (
                job_config.credentials.job_service_client.get_custom_job.call_count == 1
            )

    async def test_cancelled_worker_run(self, flow_run, job_config):
        job_display_name = "a-job-well-done"
        job_config.credentials.job_service_client.get_custom_job.return_value = (
            MagicMock(
                name="cancelled_mock_name",
                state=JobState.JOB_STATE_CANCELLED,
                error=MagicMock(message=""),
                display_name=job_display_name,
            )
        )
        async with VertexAIWorker("test-pool") as worker:
            result = await worker.run(flow_run=flow_run, configuration=job_config)
            assert (
                job_config.credentials.job_service_client.create_custom_job.call_count
                == 1
            )
            assert (
                job_config.credentials.job_service_client.get_custom_job.call_count == 1
            )
            assert result == VertexAIWorkerResult(
                status_code=1, identifier=job_display_name
            )

    async def test_missing_service_account(self, flow_run, job_config):
        job_config.service_account_name = None
        job_config.credentials._service_account_email = None
        async with VertexAIWorker("test-pool") as worker:
            with pytest.raises(
                ValueError, match="A service account is required for the Vertex job"
            ):
                await worker.run(flow_run=flow_run, configuration=job_config)
