import pytest
from prefect.utilities.dockerutils import get_prefect_image_name

from prefect_gcp.credentials import GcpCredentials
from prefect_gcp.workers.cloud_run_v2 import CloudRunWorkerJobV2Configuration


@pytest.fixture
def job_body():
    return {
        "client": "prefect",
        "launchStage": None,
        "template": {
            "template": {
                "maxRetries": None,
                "timeout": None,
                "containers": [
                    {
                        "env": [],
                        "command": None,
                        "args": "-m prefect.engine",
                        "resources": {
                            "limits": {
                                "cpu": None,
                                "memory": None,
                            },
                        },
                    },
                ],
            }
        },
    }


@pytest.fixture
def cloud_run_worker_v2_job_config(service_account_info, job_body):
    return CloudRunWorkerJobV2Configuration(
        name="my-job-name",
        job_body=job_body,
        credentials=GcpCredentials(service_account_info=service_account_info),
        region="us-central1",
        timeout=86400,
        env={"ENV1": "VALUE1", "ENV2": "VALUE2"},
    )


class TestCloudRunWorkerJobV2Configuration:
    def test_project(self, cloud_run_worker_v2_job_config):
        assert cloud_run_worker_v2_job_config.project == "my_project"

    def test_job_name(self, cloud_run_worker_v2_job_config):
        assert cloud_run_worker_v2_job_config.job_name == "prefect-my-job-name"

    def test_populate_timeout(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config._populate_timeout()

        assert (
            cloud_run_worker_v2_job_config.job_body["template"]["template"]["timeout"]
            == "86400s"
        )

    def test_populate_env(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config._populate_env()

        assert cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["env"] == [
            {"name": "ENV1", "value": "VALUE1"},
            {"name": "ENV2", "value": "VALUE2"},
        ]

    def test_populate_image_if_not_present(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config._populate_image_if_not_present()

        assert (
            cloud_run_worker_v2_job_config.job_body["template"]["template"][
                "containers"
            ][0]["image"]
            == f"docker.io/{get_prefect_image_name()}"
        )

    def test_populate_or_format_command(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config._populate_or_format_command()

        assert cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["command"] == ["python", "-m", "prefect.engine"]

    def test_format_args_if_present(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config._format_args_if_present()

        assert cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["args"] == ["-m", "prefect.engine"]
