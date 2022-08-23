from __future__ import annotations
from ast import Delete
import json
import time
from typing import Any, List, Optional, Union
from unicodedata import name
from pydantic import BaseModel
from google.api_core.client_options import ClientOptions
from google.oauth2 import service_account
from googleapiclient import discovery
import googleapiclient
from prefect.blocks.core import Block
from prefect.infrastructure.base import Infrastructure, InfrastructureResult

from prefect_gcp.credentials import GcpCredentials

class Job(BaseModel):
    metadata: dict
    spec: dict
    status: dict
    name: str
    ready_condition: dict

    def is_ready(self):
        """See if job is ready to be executed"""
        return self.ready_condition["status"] == "True"

    @classmethod
    def from_json(cls, job):
        """Construct a Job instance from a Jobs JSON response."""
        ready_condition = {}

        for condition in job["status"]["conditions"]:
            if condition["type"] == 'Ready':
                ready_condition = condition

        return cls(
            metadata = job["metadata"],
            spec = job["spec"],
            status = job["status"],
            name = job["metadata"]["name"],
            ready_condition = condition
        )

class CloudRunJobSettings(Block):
    """
    Block that contains optional settings for CloudRunJob infrastructure. It 
    does not include mandatory settings, which are found on the CloudRunJob
    itself.
    """
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
        """
        Add settings related to containers for Cloud Run Jobs to a dictionary.

        Includes environment variables, entrypoint command, entrypoint arguments,
        and cpu and memory limits.
        See: https://cloud.google.com/run/docs/reference/rest/v1/Container
        and https://cloud.google.com/run/docs/reference/rest/v1/Container#ResourceRequirements
        """
        d = self._add_env_vars(d)
        d = self._add_resources(d)
        d = self._add_command(d)
        d = self._add_args(d)

        return d

    def add_jobs_metadata_settings(self, d: dict) -> dict:
        """
        Add top-level Jobs metadata settings for Cloud Run Jobs to a dictionary.

        Includes labels.
        See: https://cloud.google.com/static/run/docs/reference/rest/v1/namespaces.jobs
        """
        return self._add_labels(d)

    def add_execution_template_spec_metadata(self, d: dict) -> dict:
        """Add settings related to the ExecutionTemplateSpec for Cloud Run Jobs
        to a dictionary. 

        Includes Cloud SQL Instances, VPC Access connector, and VPC egress.
        See: https://cloud.google.com/static/run/docs/reference/rest/v1/namespaces.jobs#ExecutionTemplateSpec
        """
        d = self._add_cloud_sql_instances(d)
        d = self._add_vpc_connector(d)
        d = self._add_vpc_egress(d)

        return d

    def _add_env_vars(self, d: dict):
        """Add environment variables for a Cloud Run Job.

        See: https://cloud.google.com/run/docs/reference/rest/v1/Container#envvar 
        """
        if self.set_env_vars is not None:
            env = [{"name": k, "value": v} for k,v in self.set_env_vars.items()]
            d["env"] = env
        
        return d

    def _add_resources(self, d: dict):
        """Set specified resources limits for a Cloud Run Job.

        See: https://cloud.google.com/run/docs/reference/rest/v1/Container#ResourceRequirements
        """
        resources = {"limits": {}}
        if self.cpu is not None:
            resources["limits"]["cpu"] = self.cpu
        if self.memory is not None:
            resources["limits"]["memory"] = self.memory
        
        if resources["limits"]:
            d["resources"] = resources
        
        return d

    def _add_command(self, d: dict):
        """Set the command that a container will run for a Cloud Run Job.

        See: https://cloud.google.com/run/docs/reference/rest/v1/Container
        """
        if self.command:
            d["command"] = self.command
        
        return d

    def _add_args(self, d: dict):
        """Set the arguments that will be passed to the entrypoint for a Cloud Run Job.

        See: https://cloud.google.com/run/docs/reference/rest/v1/Container
        """
        if self.args:
            d["args"] = self.args
        
        return d

    def _add_labels(self, d: dict) -> dict:
        """Provide labels to a Cloud Run Job.

        See: https://cloud.google.com/run/docs/reference/rest/v1/ObjectMeta
        """
        if self.labels:
            d["labels"] = self.labels
        
        return d
    
    def _add_cloud_sql_instances(self, d:dict) -> dict:
        """Set Cloud SQL connections for a Cloud Run Job.

        See: https://cloud.google.com/static/run/docs/reference/rest/v1/namespaces.jobs#ExecutionTemplateSpec
        """
        if self.set_cloud_sql_instances is not None:
            d["annotations"]["run.googleapis.com/cloudsql-instances"] = self.set_cloud_sql_instances

        return d

    def _add_vpc_connector(self, d:dict) -> dict:
        """Set a Serverless VPC Access connector for a Cloud Run Job.

        See: https://cloud.google.com/static/run/docs/reference/rest/v1/namespaces.jobs#ExecutionTemplateSpec
        """
        if self.vpc_connector is not None:
            d["annotations"]["run.googleapis.com/vpc-access-connector"] = self.vpc_connector
        
        return d

    def _add_vpc_egress(self, d:dict) -> dict:
        """Set VPC egrees for a Cloud Run Job.

        See: https://cloud.google.com/static/run/docs/reference/rest/v1/namespaces.jobs#ExecutionTemplateSpec
        """
        if self.vpc_egress is not None:
            d["annotations"]["run.googleapis.com/vpc-access-egress"] = self.vpc_egress
        
        return d

class CloudRunJob(Infrastructure):
    """Infrastructure block used to run GCP Cloud Run Jobs.

    Optional settings are available through the CloudRunJobSettings block.
    """
    type: str = "Cloud Run Job"
    job_name: str
    project_id: str
    image_url: str
    region: str
    credentials: GcpCredentials
    always_recreate: bool
    settings: Optional[CloudRunJobSettings]

    def run(self):
        with self._get_client() as jobs_client:
            job_exists = self._job_already_exists(jobs_client=jobs_client)

            if job_exists and self.always_recreate:
                self.delete_job(jobs_client=jobs_client)
                job_exists = False
                # wait until job is finished deleting 
            breakpoint()
            if not job_exists:
                # googleapiclient.errors.HttpError: <HttpError 409 when requesting 
                # https://us-east1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/helical-bongo-360018/jobs?alt=json 
                # returned "Resource 'peyton-test' already exists but was marked for deletion. Please try again once 
                # the resource is completely deleted.". Details: "Resource 'peyton-test' already exists but was 
                # marked for deletion. Please try again once the resource is completely deleted.">
                self.create_job()
            breakpoint()
            job = self._get_job(jobs_client=jobs_client)
            breakpoint()
            while not job.is_ready():
                self.logger.info(f"Job is not yet ready. Current condition: {job.ready_condition}")
                time.sleep(3)
                job = self._get_job(jobs_client=jobs_client) 

            try:
                res = self._submit_job_run(jobs_client=jobs_client)
            except googleapiclient.errors.HttpError as exc:
                self.logger.exception(f"Cloud run job received an unexpected exception when running Cloud Run Job '{self.job_name}':\n{exc!r}")
                raise
            print(res)

    def create_job(self):
        client = self._get_client()
        try:
            self._create(client)
        except googleapiclient.errors.HttpError as exc:
            # exc.status_code == 409: 
            self.logger.exception(f"Cloud run job received an unexpected exception when creating Cloud Run Job '{self.job_name}':\n{exc!r}")
            raise

    def _create(self, jobs_client):
        request = jobs_client.create(
            parent=f"namespaces/{self.project_id}", body=self._body_for_create()
        )
        response = request.execute()
        return response

    def _body_for_create(self):
        """Return the data used in the body for a Job CREATE request.

        See: https://cloud.google.com/run/docs/reference/rest/v1/namespaces.jobs
        """
        jobs_metadata = {
            "name": self.job_name,
            "annotations": {
                # See: https://cloud.google.com/run/docs/troubleshooting#launch-stage-validation
                "run.googleapis.com/launch-stage": "BETA"
            },
            "labels": self.settings.labels
        }
        self.settings.add_jobs_metadata_settings(jobs_metadata)

        execution_template_spec_metadata = {"annotations": {}}
        self.settings.add_execution_template_spec_metadata(execution_template_spec_metadata)

        containers = [
            self.settings.add_container_settings({"image": self.image_url})
        ]

        body = {
            "apiVersion": "run.googleapis.com/v1",
            "kind": "Job",
            "metadata": jobs_metadata,
            "spec": {  # JobSpec
                "template": {  # ExecutionTemplateSpec
                    "metadata": execution_template_spec_metadata,
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
        return body

    def _list_jobs(self, jobs_client):
        request = jobs_client.list(parent=f"namespaces/{self.project_id}")
        try:
            jobs = request.execute()
        except googleapiclient.errors.HttpError as exc:
            self.logger.exception(f"Cloud run job received an unexpected exception when listing Cloud Run Jobs in project '{self.project_id}':\n{exc!r}")
            raise

        return [Job.from_json(job) for job in jobs]

    def _get_job(self, jobs_client) -> Job:
        request = jobs_client.get(name=f"namespaces/{self.project_id}/jobs/{self.job_name}")
        try:
            res = request.execute()
        except googleapiclient.errors.HttpError as exc:
            self.logger.exception(f"Cloud run job received an unexpected exception when getting Job '{self.job_name}':\n{exc!r}")
            raise
        return Job.from_json(res)

    def _job_already_exists(self, jobs_client) -> List[str]:
        try:
            self._get_job(jobs_client)
        except googleapiclient.errors.HttpError as exc:
            return False
        
        return True

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

    def delete_job(self, jobs_client):
        """Called when overwrite=True
        """
        try:
            self._delete(jobs_client)
        except googleapiclient.errors.HttpError as exc:
            self.logger.exception(f"Cloud run job received an unexpected exception when deleting existing Cloud Run Job '{self.job_name}':\n{exc!r}")

    def _delete(self, jobs_client):
        request = jobs_client.delete(name=f"namespaces/{self.project_id}/jobs/{self.job_name}")
        response = request.execute()
        return response

if __name__ == "__main__":
    creds = GcpCredentials(service_account_file="creds.json")

    job = CloudRunJob(
        job_name="peyton-test",
        project_id="helical-bongo-360018",
        region="us-east1",
        image_url="gcr.io/prefect-dev-cloud2/peytons-test-image:latest",
        credentials=creds,
        always_recreate=True,
        settings=CloudRunJobSettings(
            set_env_vars = {"a": "b"},
            command=["my", "dog", "skip"],

        )
    )

    job.run()
