from __future__ import annotations
import json
from typing import Any, List, Optional, Union
from pydantic import BaseModel
from google.api_core.client_options import ClientOptions
from google.oauth2 import service_account
from googleapiclient import discovery
from prefect.blocks.core import Block
from prefect.infrastructure.base import Infrastructure, InfrastructureResult

from prefect_gcp.credentials import GcpCredentials


class CloudRunJobSetting(Block):
    args: Optional[List[str]] = None # done
    command: Optional[List[str]] = None # done
    cpu: Optional[Any] = None # done
    labels: Optional[List[str]] = None # done
    memory: Optional[Any] = None # done
    set_cloud_sql_instances: Optional[List] = None # done
    set_env_vars: Optional[dict] = None # done
    set_secrets: Optional[Any] = None #TODO how does this work?
    vpc_connector: Optional[Any] = None #TODO how does this work? # done
    vpc_egress: Optional[Any] = None #TODO how does this work? # done

    def add_container_settings(self, d: dict) -> dict:
        d = self._add_env_vars(d)
        d = self._add_resources(d)
        d = self._add_command(d)
        d = self._add_args(d)

        return d

    def _add_env_vars(self, d: dict):
        if self.set_env_vars is not None:
            env = [{"name": k, "value": v} for k,v in self.set_env_vars.items()]
            d["env"] = env
        
        return d


    def _add_resources(self, d: dict):
        resources = {"limits": {}}
        if self.cpu is not None:
            resources["limits"]["cpu"] = self.cpu
        if self.memory is not None:
            resources["limits"]["memory"] = self.memory
        
        if resources["limits"]:
            d["resources"] = resources
        
        return d

    def _add_command(self, d: dict):
        if self.command:
            d["command"] = self.command
        
        return d

    def _add_args(self, d: dict):
        if self.args:
            d["args"] = self.args
        
        return d
class CloudRunJob(Infrastructure):
    type: str = "Cloud Run Job"
    job_name: str
    project_id: str
    image_url: str
    region: str
    credentials: GcpCredentials
    settings: Optional[CloudRunJobSetting]

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
        # See: https://cloud.google.com/run/docs/reference/rest/v1/namespaces.jobs#Job


        metadata_1 = {
            "name": self.job_name,
            "annotations": {
                # See: https://cloud.google.com/run/docs/troubleshooting#launch-stage-validation
                "run.googleapis.com/launch-stage": "BETA"
            },
            "labels": self.settings.labels
        }

        # metadata_2 = {
        #     "run.googleapis.com/cloudsql-instances": self.settings.set_cloud_sql_instances,
        #     "run.googleapis.com/vpc-access-connector": self.settings.vpc_connector,
        #     "run.googleapis.com/vpc-access-egress": self.settings.vpc_egress
        # }
        metadata_2 = {}

        # resources = {
        #     "limits": {
        #         "cpu": self.settings.cpu,
        #         "memory": self.settings.memory
        #     }
        # }
        containers = [
            self.settings.add_container_settings({"image": self.image_url})
        ]

        job = {
            "apiVersion": "run.googleapis.com/v1",
            "kind": "Job",
            "metadata": metadata_1,
            "spec": {  # JobSpec
                "template": {  # ExecutionTemplateSpec
                    "metadata": metadata_2,
                    "spec": {  # ExecutionSpec
                        "template": {  # TaskTemplateSpec
                            "spec": {  # TaskSpec
                                "containers": containers
                            }
                        }
                    }
                }
            },
        }
        return job

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
    # registry = GoogleCloudRegistry(
    #     username="_json_key",
    #     password="",
    #     registry_url="gcr.io/helical-bongo-360018",
    #     credentials=creds,
    # )
    # registry.login()

    job = CloudRunJob(
        job_name="peyton-test1",
        project_id="helical-bongo-360018",
        region="us-east1",
        image_url="gcr.io/prefect-dev-cloud2/peytons-test-image:latest",
        credentials=creds,
        settings=CloudRunJobSetting(
            set_env_vars = {"a": "b"},
            command=["my", "dog", "skip"]
        )
    )

    job.create()
