from unittest.mock import MagicMock

import pytest
from google.cloud.aiplatform_v1.types.job_state import JobState
from prefect.exceptions import InfrastructureNotFound

from prefect_gcp.aiplatform import (
    VertexAICustomTrainingJob,
    VertexAICustomTrainingJobResult,
)


class TestVertexAICustomTrainingJob:
    @pytest.fixture
    def vertex_ai_custom_training_job(self, gcp_credentials):
        return VertexAICustomTrainingJob(
            command=["echo", "hello!!"],
            region="us-east1",
            image="us-docker.pkg.dev/cloudrun/container/job:latest",
            gcp_credentials=gcp_credentials,
        )

    # TODO: Improve test resiliency to changes in str output
    def test_preview(self, vertex_ai_custom_training_job: VertexAICustomTrainingJob):
        actual_lines = vertex_ai_custom_training_job.preview().splitlines()

        base_env = VertexAICustomTrainingJob._base_environment().copy()
        env_lines = [
            f'env {{\n        name: "{name}"\n        value: "{value}"\n      }}'
            for name, value in base_env.items()
        ]
        env_str = "\n".join(env_lines)

        expected_lines = f"""
            display_name: "container
            job_spec {{
                worker_pool_specs {{
                    container_spec {{
                        image_uri: "us-docker.pkg.dev/cloudrun/container/job:latest"
                        command: "echo"
                        command: "hello!!"
                        {env_str}
                    }}
                    machine_spec {{
                        machine_type: "n1-standard-4"
                    }}
                    replica_count: 1
                }}
                scheduling {{
                }}
                service_account: "my_service_account_email"
            }}
        """.strip().splitlines()

        for actual_line, expected_line in zip(actual_lines, expected_lines):
            if '"container' in actual_line:
                actual_line = actual_line.split("-")[0]  # remove the unique hex
            assert actual_line.strip() == expected_line.strip()  # disregard whitespace

    def test_environment_variables(self, gcp_credentials):
        vertex_job = VertexAICustomTrainingJob(
            command=["echo", "hello!!"],
            region="us-east1",
            image="us-docker.pkg.dev/cloudrun/container/job:latest",
            gcp_credentials=gcp_credentials,
            env={"FOO": "BAR"},
        )
        job_spec = vertex_job._build_job_spec()

        assert len(job_spec.worker_pool_specs) == 1

        env_list_in_container_spec = job_spec.worker_pool_specs[0].container_spec.env

        expected_env = VertexAICustomTrainingJob._base_environment().copy()
        expected_env.update({"FOO": "BAR"})

        for item in env_list_in_container_spec:
            assert item.name in expected_env
            assert item.value == expected_env[item.name]

    def test_kill(
        self, vertex_ai_custom_training_job: VertexAICustomTrainingJob, caplog
    ):
        identifier = "projects/1234/locations/us-east1/customJobs/12345"
        vertex_ai_custom_training_job.kill(identifier)
        for record in caplog.records:
            if f"Requested to cancel {identifier}..." in record.msg:
                break
        else:
            raise AssertionError("identifier not in caplog")

    def raise_not_found(self, request):
        raise RuntimeError("Job does not exist")

    def test_kill_infrastructure_not_found(
        self, vertex_ai_custom_training_job: VertexAICustomTrainingJob
    ):
        identifier = "projects/1234/locations/us-east1/customJobs/12345"
        gcp_credentials = vertex_ai_custom_training_job.gcp_credentials
        job_service_client = gcp_credentials.job_service_client
        job_service_client.cancel_custom_job.side_effect = self.raise_not_found
        with pytest.raises(
            InfrastructureNotFound, match="Cannot stop Vertex AI job; the job name"
        ):
            vertex_ai_custom_training_job.kill(identifier)

    def raise_random_error(self, request):
        raise RuntimeError("Random error")

    def test_kill_infrastructure_error(
        self, vertex_ai_custom_training_job: VertexAICustomTrainingJob
    ):
        identifier = "projects/1234/locations/us-east1/customJobs/12345"
        gcp_credentials = vertex_ai_custom_training_job.gcp_credentials
        job_service_client = gcp_credentials.job_service_client
        job_service_client.cancel_custom_job.side_effect = self.raise_random_error
        with pytest.raises(RuntimeError, match="Random error"):
            vertex_ai_custom_training_job.kill(identifier)

    def test_run(self, vertex_ai_custom_training_job: VertexAICustomTrainingJob):
        actual = vertex_ai_custom_training_job.run()
        expected = VertexAICustomTrainingJobResult(
            identifier="mock_display_name", status_code=0
        )
        assert actual == expected

    def test_run_error(self, vertex_ai_custom_training_job: VertexAICustomTrainingJob):
        error = MagicMock(message="my error msg")
        failed_run_final = MagicMock(
            name="mock_name",
            state=JobState.JOB_STATE_FAILED,
            error=error,
            display_name="mock_display_name",
        )
        gcp_credentials = vertex_ai_custom_training_job.gcp_credentials
        gcp_credentials.job_service_client.get_custom_job.return_value = (
            failed_run_final
        )
        with pytest.raises(RuntimeError, match="my error msg"):
            vertex_ai_custom_training_job.run()
