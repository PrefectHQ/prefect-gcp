import uuid

import pytest
from prefect.client.schemas import FlowRun

from prefect_gcp.credentials import GcpCredentials
from prefect_gcp.vertex_worker import VertexAIWorkerJobConfiguration


@pytest.fixture
def vertex_worker_job_config(service_account_info):
    return VertexAIWorkerJobConfiguration(
        name="my-custom-ai-job",
        image="gcr.io/your-project/your-repo:latest",
        region="ashenvale",
        credentials=GcpCredentials(
            service_account_info=service_account_info, project="your-project"
        ),
    )


@pytest.fixture
def flow_run():
    return FlowRun(flow_id=uuid.uuid4(), name="my-flow-run-name")


class TestVertexAIWorkerJobConfiguration:
    def test_gcp_project(
        self, vertex_worker_job_config: VertexAIWorkerJobConfiguration
    ):
        assert vertex_worker_job_config.project == "your-project"

    def test_job_name(self, vertex_worker_job_config: VertexAIWorkerJobConfiguration):
        assert vertex_worker_job_config.job_name.startswith("your-repo:latest")

    def test_command_as_list(
        self, vertex_worker_job_config: VertexAIWorkerJobConfiguration
    ):
        assert [
            "python",
            "-m",
            "prefect.engine",
        ] == vertex_worker_job_config.command_as_list()

        vertex_worker_job_config.command = "echo -n hello"
        assert ["echo", "-n", "hello"] == vertex_worker_job_config.command_as_list()


# class TestVertexAIWorker:
#     async def test_successful_worker_run(self, flow_run, vertex_worker_job_config):
#         async with VertexAIWorker("test-pool") as worker:
#           await worker.run(flow_run=flow_run, configuration=vertex_worker_job_config)
