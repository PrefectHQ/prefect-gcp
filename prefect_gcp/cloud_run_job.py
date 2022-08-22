import json
from typing import Any, Optional, Union

from google.api_core.client_options import ClientOptions
from google.oauth2 import service_account
from googleapiclient import discovery
from prefect.blocks.core import Block
from prefect.infrastructure.base import Infrastructure, InfrastructureResult

from prefect_gcp.cloud_registry import GoogleCloudRegistry
from prefect_gcp.credentials import GcpCredentials


class CloudRunJobSetting(Block):
    pass


class CloudRunJob(Infrastructure):
    job_name: str
    type: str = "Cloud Run Job"
    project_id: str
    region: str
    image_url: Optional[str] = None
    image_name: Optional[str] = None
    credentials: GcpCredentials
    registry: Optional[Any]

    def validate_something_or_another(self):
        if not self.image_name and not self.image_url:
            raise ValueError(
                "You must provide either an image URL or an image name for a cloud run job."
            )

    def run(self):
        with self._get_client() as jobs_client:
            # self._register(jobs_client=jobs_client)
            res = self._submit_job_run(jobs_client=jobs_client)
            print(res)

    def create(self):
        client = self._get_client()
        self._create(client)

    def _create(self, jobs_client):
        request = jobs_client.create(
            parent=f"namespaces/{self.project_id}", body=self._create_request_body()
        )
        response = request.execute()
        return response

    def _create_request_body(self):
        # https://cloud.google.com/run/docs/reference/rest/v1/namespaces.jobs#Job
        body = {
            "apiVersion": "run.googleapis.com/v1",
            "kind": "Job",
            "metadata": {
                "name": self.job_name,
                "annotations": {
                    # https://cloud.google.com/run/docs/troubleshooting#launch-stage-validation
                    "run.googleapis.com/launch-stage": "BETA"
                },
            },
            "spec": {  # JobSpec
                "template": {  # ExecutionTemplateSpec
                    "spec": {  # ExecutionSpec
                        "template": {  # TaskTemplateSpec
                            "spec": {  # TaskSpec
                                "containers": [{"image": self.image_url}]
                            }
                        }
                    }
                }
            },
        }
        return body

    def _check_registry(self):
        pass

    def _submit_job_run(self, jobs_client):
        request = jobs_client.run(
            name=f"namespaces/{self.project_id}/jobs/{self.job_name}"
        )
        response = request.execute()
        return response

    def _get_client(self):
        # region needed for 'v1' API
        api_endpoint = f"https://{self.region}-run.googleapis.com"
        credentials = self.credentials.get_credentials_from_service_account()
        options = ClientOptions(api_endpoint=api_endpoint)

        return (
            discovery.build(
                "run", "v1", client_options=options, credentials=credentials
            )
            .namespaces()
            .jobs()
        )

    def preview(self):
        pass


if __name__ == "__main__":
    creds = GcpCredentials(service_account_file="creds.json")
    registry = GoogleCloudRegistry(
        username="_json_key",
        password="",
        registry_url="gcr.io/helical-bongo-360018",
        credentials=creds,
    )
    registry.login()

    job = CloudRunJob(
        job_name="peyton-test",
        project_id="helical-bongo-360018",
        region="us-east1",
        image_url="gcr.io/prefect-dev-cloud2/peytons-test-image:latest",
        # image_name = 'gcr.io/helical-bongo-360018/peytons-test-image',
        credentials=creds,
        registry=registry,
    )

    job.register()
