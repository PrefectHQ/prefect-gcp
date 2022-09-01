
"""
Examples:
    Run a job using Google Cloud Run Jobs
    >>> CloudRunJob(
    >>>     image="gcr.io/my-project/my-image",
    >>>     region="us-east1",
    >>>     credentials=my_gcp_credentials
    >>> ).run()
    Run a job that runs the command `echo hello world` using Google Cloud Run Jobs
    >>> CloudRunJob(
    >>>     image="gcr.io/my-project/my-image",
    >>>     region="us-east1",
    >>>     credentials=my_gcp_credentials
    >>>     command=["echo", "hello world"]
    >>> ).run()
"""
from __future__ import annotations
import datetime
from ast import Delete
from importlib.metadata import metadata
import time
from typing import Any, List, Literal, Optional, Union
from unicodedata import name
from uuid import uuid4
from pydantic import BaseModel, Field, root_validator, validator
from google.api_core.client_options import ClientOptions
from google.oauth2 import service_account
from googleapiclient import discovery
import googleapiclient
from prefect.blocks.core import Block
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from google.cloud import logging
from google.cloud.logging import TextEntry, ProtobufEntry
from google.cloud.logging import ASCENDING
import pytz
from prefect_gcp.credentials import GcpCredentials
from prefect.docker import get_prefect_image_name
from anyio.abc import TaskStatus
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible

class CloudRunJobResult(InfrastructureResult):
    """Result from a Cloud Run Job."""

class Job(BaseModel):
    metadata: dict
    spec: dict
    status: dict
    name: str
    ready_condition: dict
    execution_status: dict

    def is_ready(self):
        """Whether a job is finished registering and ready to be executed"""
        return self.ready_condition.get("status") == "True"

    def has_execution_in_progress(self):
        """See if job has a run in progress."""
        return (
            self.execution_status and
            self.execution_status.get("completionTimestamp") is not None
        )

    @staticmethod
    def _get_ready_condition(job):
        ready_condition = {}

        if job["status"].get("conditions"):
            for condition in job["status"]["conditions"]:
                if condition["type"] == 'Ready':
                    ready_condition = condition
        
        return ready_condition

    @staticmethod 
    def _get_execution_status(job):
        if job["status"].get("latestCreatedExecution"):
            return job["status"]["latestCreatedExecution"]
        
        return {}
    
    @classmethod
    def from_json(cls, job):
        """Construct a Job instance from a Jobs JSON response."""

        return cls(
            metadata = job["metadata"],
            spec = job["spec"],
            status = job["status"],
            name = job["metadata"]["name"],
            ready_condition = cls._get_ready_condition(job),
            execution_status = cls._get_execution_status(job)
        )

class Execution(BaseModel):
    name: str
    metadata: dict
    spec: dict
    status: dict
    log_uri: str

    def is_running(self) -> bool:
        """Returns True if Execution is not completed."""
        return self.status.get("completionTime") is None

    def condition_after_completion(self):
        """Returns Execution condition if Execution has completed."""
        for condition in self.status["conditions"]:
            if condition["type"] == "Completed":
                return condition

    def succeeded(self):
        """Whether or not the Execution completed is a successful state."""
        completed_condition = self.condition_after_completion()
        if completed_condition and completed_condition["status"] == "True":
            return True
        
        return False

    @classmethod
    def from_json(cls, execution):
        """Create an Execution instance from a GCP Execution API request."""
        return cls(
            name = execution['metadata']['name'],
            metadata = execution['metadata'],
            spec = execution['spec'],
            status = execution['status'],
            log_uri = execution['status'].get('logUri'),
        )

class CloudRunJob(Infrastructure):
    """Infrastructure block used to run GCP Cloud Run Jobs.
    """
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/4CD4wwbiIKPkZDt4U3TEuW/c112fe85653da054b6d5334ef662bec4/gcp.png?h=250"  # noqa
    _block_type_name = "Cloud Run Job"

    type: Literal["cloud-run-job"] = Field(
        "cloud-run-job", description="The slug for this task type."
    )
    image: str = Field(
        description=(
            "The image to use for the Cloud Run Job. This value must"
            "refer to an image within either Google Container Registry"
            "or Google Artifact Registry."
        ),
    )

    region: str
    credentials: GcpCredentials

    # Job settings
    cpu: Optional[str] = None 
    memory: Optional[str] = None 

    args: Optional[List[str]] = None 
    env: dict[str, str] = Field(default_factory=dict)
    stream_output: bool = False

    # Cleanup behavior
    keep_job_after_completion: Optional[bool] = False

    # For private use
    _job_name: str = None
    _execution: Optional[Execution] = None
    _project_id: str = None

    @property
    def project_id(self):
        if self._project_id is None:
            self._project_id = self.credentials.get_project_id()
        
        return self._project_id

    @property
    def job_name(self):
        """Create a unique and valid job name."""
        if self._job_name is None:
            components = self.image.split("/")
            #gcr.io/<project_name>/repo/whatever
            image_name = components[2]
            modified_image_name = image_name.replace((":"),"-").replace(("."),"-") # only alphanumeric and '-' allowed
            self._job_name = f"{modified_image_name}-{uuid4()}"
        
        return self._job_name

    @validator("image")
    def remove_spaces(cls, value):
        """Deal with sneaky spaces in image names (hard to see on UI)."""
        return value.replace(" ", "")

    @sync_compatible
    async def run(self, task_status: Optional[TaskStatus] = None):
        with self._get_jobs_client() as jobs_client:
            try:
                #TODO check if container actually exists
                self.logger.info(f"Creating Cloud Run Job {self.job_name}")
                self._create_job(client=jobs_client)
            except Exception as exc:
                self.logger.exception(f"Encountered an unexpected error when creating Cloud Run Job {self.job_name}:\n{exc!r}")
                raise

            self._wait_for_job_creation(client=jobs_client)

            if task_status:
                task_status.started(self.job_name) 

            return await run_sync_in_worker_thread(
                self._watch_job_and_get_result,
                jobs_client,
                5,
            )

    def _create_job(self, client):
        """Create a new Cloud Run Job."""
        try:
            self._create(client, body=self._body_for_create())
        except googleapiclient.errors.HttpError as exc:
            # exc.status_code == 409: 
            self.logger.exception(f"Cloud run job received an unexpected exception when creating Cloud Run Job '{self.job_name}':\n{exc!r}")
            raise

    def _create(self, client, body):
        """Submit a create request to Cloud Run Job API."""
        request = client.create(
            parent=f"namespaces/{self.project_id}", body=body
        )
        response = request.execute()
        return response

    def _body_for_create(self):
        """Create properly formatted body used for a Job CREATE request.
        See: https://cloud.google.com/run/docs/reference/rest/v1/namespaces.jobs
        """
        jobs_metadata = {
            "name": self.job_name,
            "annotations": {
                # See: https://cloud.google.com/run/docs/troubleshooting#launch-stage-validation
                "run.googleapis.com/launch-stage": "BETA"
            },
        }

        execution_template_spec_metadata = {"annotations": {}}

        # env and command here
        containers = [
            self._add_container_settings({"image": self.image})
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

    def _get_job(self, client) -> Job:
        """Get the Job associated with the CloudRunJob."""
        request = client.get(name=f"namespaces/{self.project_id}/jobs/{self.job_name}")
        res = request.execute()
        return Job.from_json(res)

    def _submit_job_for_execution(self, client):
        """Submit a request to begin a new run of the Cloud Run Job."""
        request = client.run(
            name=f"namespaces/{self.project_id}/jobs/{self.job_name}"
        )
        response = request.execute()
        return Execution.from_json(response)

    def _get_client(self):
        """Get the base client needed for interacting with GCP APIs."""
        # region needed for 'v1' API
        api_endpoint = f"https://{self.region}-run.googleapis.com"
        credentials = self.credentials.get_credentials_from_service_account()
        options = ClientOptions(api_endpoint=api_endpoint)

        return (
            discovery.build(
                "run", "v1", client_options=options, credentials=credentials
            )
            .namespaces()
        )

    def _get_jobs_client(self):
        """Get the client needed for interacting with Cloud Run Jobs."""
        return self._get_client().jobs()

    def _get_executions_client(self):
        """Get the client needed for interacting with container executions."""
        return self._get_client().executions()

    def preview(self):
        pass

    def _delete_job(self, client):
        """Make a delete request for the Cloud Run Job."""
        request = client.delete(name=f"namespaces/{self.project_id}/jobs/{self.job_name}")
        response = request.execute()
        return response

    def _watch_job_execution(self, job_execution: Execution, poll_interval=5):
        """Update job_execution status until it is no longer running."""
        client = self._get_executions_client()

        while job_execution.is_running():
            time.sleep(poll_interval)

            job_execution = Execution.from_json(
                client.get(
                    name=f"namespaces/{job_execution.metadata['namespace']}/executions/{job_execution.metadata['name']}"
                ).execute()
            )

        return job_execution

    def _wait_for_job_creation(self, client, poll_interval=5):
        """Give created job time to register"""
        job = self._get_job(client=client) 

        while not job.is_ready():
            ready_condition = job.ready_condition if job.ready_condition else "waiting for condition update"
            self.logger.info(f"Job is not yet ready... Current condition: {ready_condition}")
            time.sleep(poll_interval)

            job = self._get_job(client=client) 

    def _add_args(self, d: dict):
        """Set the arguments that will be passed to the entrypoint for a Cloud Run Job.
        See: https://cloud.google.com/run/docs/reference/rest/v1/Container
        """
        if self.args:
            d["args"] = self.args
        
        return d

    def _add_command(self, d: dict):
        """Set the command that a container will run for a Cloud Run Job.
        See: https://cloud.google.com/run/docs/reference/rest/v1/Container
        """
        d["command"] = self.command
        
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

    def _add_env(self, d: dict):
        """Add environment variables for a Cloud Run Job.

        Method `self._base_environment()` gets necessary Prefect environment variables
        from the config.

        See: https://cloud.google.com/run/docs/reference/rest/v1/Container#envvar for 
        how environment variables are specified for Cloud Run Jobs.
        """
        env = {**self._base_environment(), **self.env}
        cloud_run_job_env = [{"name": k, "value": v} for k,v in env.items()]
        d["env"] = cloud_run_job_env
        return d

    def _add_container_settings(self, d: dict) -> dict:
        """
        Add settings related to containers for Cloud Run Jobs to a dictionary.
        Includes environment variables, entrypoint command, entrypoint arguments,
        and cpu and memory limits.
        See: https://cloud.google.com/run/docs/reference/rest/v1/Container
        and https://cloud.google.com/run/docs/reference/rest/v1/Container#ResourceRequirements
        """
        d = self._add_env(d)
        d = self._add_resources(d)
        d = self._add_command(d)
        d = self._add_args(d)

        return d

    def _watch_job_and_get_result(self, client, poll_interval):
        try:
            self.logger.info(f"Submitting Cloud Run Job {self.job_name} for execution.")
            job_execution = self._submit_job_for_execution(client=client)
        except Exception as exc:
            self.logger.exception(f"Received an unexpected exception when submitting Cloud Run Job '{self.job_name}':\n{exc!r}")
        
        command = ' '.join(self.command) if self.command else "'default container command'"

        self.logger.info(
            f"Cloud Run Job {self.job_name}: Running command '{command}'"
        )

        try:
            job_execution = self._watch_job_execution(job_execution=job_execution, poll_interval=poll_interval)
        except Exception as exc:
            self.logger.exception(f"Received an unexpected exception while monitoring Cloud Run Job '{self.job_name}':\n{exc!r}")
            raise

        if job_execution.succeeded():
            status_code = 0
            self.logger.info(f"Job Run {self.job_name} completed successfully")
        else:
            status_code = 1
            self.logger.error(f"Job Run {self.job_name} did not complete successfully. {job_execution.condition_after_completion()['message']}")

        self.logger.info(f"Job Run logs can be found on GCP at: {job_execution.log_uri}") 

        if not self.keep_job_after_completion:
            try:
                self.logger.info(f"Deleting completed Cloud Run Job {self.job_name} from Google Cloud Run...")
                self._delete_job(client=client)
            except googleapiclient.errors.HttpError as Exc:
                self.logger.exception(f"Received an unexpected exception while attempting to delete completed Cloud Run Job.'{self.job_name}':\n{exc!r}")

        return CloudRunJobResult(identifier=self.job_name, status_code=status_code)
