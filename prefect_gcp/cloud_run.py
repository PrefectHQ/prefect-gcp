"""
<span class="badge-api experimental"/>

Integrations with Google Cloud Run Job.

Note this module is experimental. The intefaces within may change without notice.

Examples:

    Run a job using Google Cloud Run Jobs:
    ```python
    CloudRunJob(
        image="gcr.io/my-project/my-image",
        region="us-east1",
        credentials=my_gcp_credentials
    ).run()
    ```

    Run a job that runs the command `echo hello world` using Google Cloud Run Jobs:
    ```python
    CloudRunJob(
        image="gcr.io/my-project/my-image",
        region="us-east1",
        credentials=my_gcp_credentials
        command=["echo", "hello world"]
    ).run()
    ```

"""
from __future__ import annotations

import json
import time
from typing import Dict, List, Optional

import googleapiclient
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from googleapiclient.discovery import Resource
from prefect.exceptions import InfrastructureNotFound
from pydantic import Field
from typing_extensions import Literal

from prefect_gcp.base_cloud_run import (
    BaseCloudRunJob,
    BaseCloudRunJobResult,
    BaseExecution,
    BaseJob,
)
from prefect_gcp.credentials import GcpCredentials


class Job(BaseJob):
    """
    Utility class to call GCP `jobs` API and
    interact with the returned objects.
    """

    metadata: Dict
    spec: Dict
    status: Dict
    name: str
    ready_condition: Dict
    execution_status: Dict

    def is_ready(self) -> bool:
        """Whether a job is finished registering and ready to be executed"""
        if self._is_missing_container():
            raise Exception(f"{self.ready_condition['message']}")
        return self.ready_condition.get("status") == "True"

    def _is_missing_container(self) -> bool:
        """
        Check if Job status is not ready because
        the specified container cannot be found.
        """
        if (
            self.ready_condition.get("status") == "False"
            and self.ready_condition.get("reason") == "ContainerMissing"
        ):
            return True
        return False

    def has_execution_in_progress(self) -> bool:
        """See if job has a run in progress."""
        return (
            self.execution_status == {}
            or self.execution_status.get("completionTimestamp") is None
        )

    @staticmethod
    def _get_ready_condition(job: Dict) -> Dict:
        """Utility to access JSON field containing ready condition."""
        if job["status"].get("conditions"):
            for condition in job["status"]["conditions"]:
                if condition["type"] == "Ready":
                    return condition

        return {}

    @staticmethod
    def _get_execution_status(job: Dict):
        """Utility to access JSON field containing execution status."""
        if job["status"].get("latestCreatedExecution"):
            return job["status"]["latestCreatedExecution"]

        return {}

    @classmethod
    def get(cls, client: Resource, namespace: str, job_name: str):
        """Make a get request to the GCP jobs API and return a Job instance."""
        request = client.jobs().get(name=f"namespaces/{namespace}/jobs/{job_name}")
        response = request.execute()

        return cls(
            metadata=response["metadata"],
            spec=response["spec"],
            status=response["status"],
            name=response["metadata"]["name"],
            ready_condition=cls._get_ready_condition(response),
            execution_status=cls._get_execution_status(response),
        )

    @staticmethod
    def create(client: Resource, namespace: str, body: Dict):
        """Make a create request to the GCP jobs API."""
        request = client.jobs().create(parent=f"namespaces/{namespace}", body=body)
        response = request.execute()
        return response

    @staticmethod
    def delete(client: Resource, namespace: str, job_name: str):
        """Make a delete request to the GCP jobs API."""
        request = client.jobs().delete(name=f"namespaces/{namespace}/jobs/{job_name}")
        response = request.execute()
        return response

    @staticmethod
    def run(client: Resource, namespace: str, job_name: str):
        """Make a run request to the GCP jobs API."""
        request = client.jobs().run(name=f"namespaces/{namespace}/jobs/{job_name}")
        response = request.execute()
        return response


class Execution(BaseExecution):
    """
    Utility class to call GCP `executions` API and
    interact with the returned objects.
    """

    name: str
    namespace: str
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
    def get(cls, client: Resource, namespace: str, execution_name: str):
        """
        Make a get request to the GCP executions API
        and return an Execution instance.
        """
        request = client.executions().get(
            name=f"namespaces/{namespace}/executions/{execution_name}"
        )
        response = request.execute()

        return cls(
            name=response["metadata"]["name"],
            namespace=response["metadata"]["namespace"],
            metadata=response["metadata"],
            spec=response["spec"],
            status=response["status"],
            log_uri=response["status"]["logUri"],
        )


class CloudRunJobResult(BaseCloudRunJobResult):
    """Result from a Cloud Run Job."""


class CloudRunJob(BaseCloudRunJob):
    """
    <span class="badge-api experimental"/>

    Infrastructure block used to run GCP Cloud Run Jobs.

    Project name information is provided by the Credentials object, and should always
    be correct as long as the Credentials object is for the correct project.

    Note this block is experimental. The interface may change without notice.
    """

    _block_type_slug = "cloud-run-job"
    _block_type_name = "GCP Cloud Run Job"
    _description = "Infrastructure block used to run GCP Cloud Run Jobs. Note this block is experimental. The interface may change without notice."  # noqa
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/4CD4wwbiIKPkZDt4U3TEuW/c112fe85653da054b6d5334ef662bec4/gcp.png?h=250"  # noqa
    _documentation_url = "https://prefecthq.github.io/prefect-gcp/cloud_run/#prefect_gcp.cloud_run.CloudRunJob"  # noqa: E501

    type: Literal["cloud-run-job"] = Field(
        "cloud-run-job", description="The slug for this task type."
    )
    image: str = Field(
        ...,
        title="Image Name",
        description=(
            "The image to use for a new Cloud Run Job. This value must "
            "refer to an image within either Google Container Registry "
            "or Google Artifact Registry, like `gcr.io/<project_name>/<repo>/`."
        ),
    )
    region: str = Field(..., description="The region where the Cloud Run Job resides.")
    credentials: GcpCredentials  # cannot be Field; else it shows as Json

    # Job settings
    cpu: Optional[int] = Field(
        default=None,
        title="CPU",
        description=(
            "The amount of compute allocated to the Cloud Run Job. "
            "The int must be valid based on the rules specified at "
            "https://cloud.google.com/run/docs/configuring/cpu#setting-jobs ."
        ),
    )
    memory: Optional[int] = Field(
        default=None,
        title="Memory",
        description="The amount of memory allocated to the Cloud Run Job.",
    )
    memory_unit: Optional[Literal["G", "Gi", "M", "Mi"]] = Field(
        default=None,
        title="Memory Units",
        description=(
            "The unit of memory. See "
            "https://cloud.google.com/run/docs/configuring/memory-limits#setting "
            "for additional details."
        ),
    )
    vpc_connector_name: Optional[str] = Field(
        default=None,
        title="VPC Connector Name",
        description="The name of the VPC connector to use for the Cloud Run Job.",
    )
    args: Optional[List[str]] = Field(
        default=None,
        description=(
            "Arguments to be passed to your Cloud Run Job's entrypoint command."
        ),
    )
    env: Dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables to be passed to your Cloud Run Job.",
    )

    # Cleanup behavior
    keep_job: Optional[bool] = Field(
        default=False,
        title="Keep Job After Completion",
        description="Keep the completed Cloud Run Job on Google Cloud Platform.",
    )
    timeout: Optional[int] = Field(
        default=600,
        gt=0,
        le=3600,
        title="Job Timeout",
        description=(
            "The length of time that Prefect will wait for a Cloud Run Job to complete "
            "before raising an exception."
        ),
    )
    # For private use
    _job_name: str = None
    _execution: Optional[Execution] = None

    def _kill_job(self, client: Resource, namespace: str, job_name: str) -> None:
        """
        Thin wrapper around Job.delete, wrapping a try/except since
        Job is an independent class that doesn't have knowledge of
        CloudRunJob and its associated logic.
        """
        try:
            Job.delete(client=client, namespace=namespace, job_name=job_name)
        except Exception as exc:
            if "does not exist" in str(exc):
                raise InfrastructureNotFound(
                    f"Cannot stop Cloud Run Job; the job name {job_name!r} "
                    "could not be found."
                ) from exc
            raise

    def _create_job_and_wait_for_registration(self, client: Resource) -> None:
        """Create a new job wait for it to finish registering."""
        try:
            self.logger.info(f"Creating Cloud Run Job {self.job_name}")
            Job.create(
                client=client,
                namespace=self.credentials.project,
                body=self._jobs_body(),
            )
        except googleapiclient.errors.HttpError as exc:
            self._create_job_error(exc)

        try:
            self._wait_for_job_creation(client=client, timeout=self.timeout)
        except Exception:
            self.logger.exception(
                "Encountered an exception while waiting for job run creation"
            )
            if not self.keep_job:
                self.logger.info(
                    f"Deleting Cloud Run Job {self.job_name} from Google Cloud Run."
                )
                try:
                    Job.delete(
                        client=client,
                        namespace=self.credentials.project,
                        job_name=self.job_name,
                    )
                except Exception:
                    self.logger.exception(
                        "Received an unexpected exception while attempting to delete"
                        f" Cloud Run Job {self.job_name!r}"
                    )
            raise

    def _begin_job_execution(self, client: Resource) -> Execution:
        """Submit a job run for execution and return the execution object."""
        try:
            self.logger.info(
                f"Submitting Cloud Run Job {self.job_name!r} for execution."
            )
            submission = Job.run(
                client=client,
                namespace=self.credentials.project,
                job_name=self.job_name,
            )

            job_execution = Execution.get(
                client=client,
                namespace=submission["metadata"]["namespace"],
                execution_name=submission["metadata"]["name"],
            )

            command = (
                " ".join(self.command) if self.command else "default container command"
            )

            self.logger.info(
                f"Cloud Run Job {self.job_name!r}: Running command {command!r}"
            )
        except Exception as exc:
            self._job_run_submission_error(exc)

        return job_execution

    def _watch_job_execution_and_get_result(
        self, client: Resource, execution: Execution, poll_interval: int
    ) -> CloudRunJobResult:
        """Wait for execution to complete and then return result."""
        try:
            job_execution = self._watch_job_execution(
                client=client,
                job_execution=execution,
                timeout=self.timeout,
                poll_interval=poll_interval,
            )
        except Exception:
            self.logger.exception(
                "Received an unexpected exception while monitoring Cloud Run Job "
                f"{self.job_name!r}"
            )
            raise

        if job_execution.succeeded():
            status_code = 0
            self.logger.info(f"Job Run {self.job_name} completed successfully")
        else:
            status_code = 1
            error_msg = job_execution.condition_after_completion()["message"]
            self.logger.error(
                f"Job Run {self.job_name} did not complete successfully. {error_msg}"
            )

        self.logger.info(
            f"Job Run logs can be found on GCP at: {job_execution.log_uri}"
        )

        if not self.keep_job:
            self.logger.info(
                f"Deleting completed Cloud Run Job {self.job_name!r} from Google Cloud"
                " Run..."
            )
            try:
                Job.delete(
                    client=client,
                    namespace=self.credentials.project,
                    job_name=self.job_name,
                )
            except Exception:
                self.logger.exception(
                    "Received an unexpected exception while attempting to delete Cloud"
                    f" Run Job {self.job_name}"
                )

        return CloudRunJobResult(identifier=self.job_name, status_code=status_code)

    def _jobs_body(self) -> Dict:
        """Create properly formatted body used for a Job CREATE request.
        See: https://cloud.google.com/run/docs/reference/rest/v1/namespaces.jobs
        """
        jobs_metadata = {"name": self.job_name}

        annotations = {
            # See: https://cloud.google.com/run/docs/troubleshooting#launch-stage-validation  # noqa
            "run.googleapis.com/launch-stage": "BETA",
        }
        # add vpc connector if specified
        if self.vpc_connector_name:
            annotations[
                "run.googleapis.com/vpc-access-connector"
            ] = self.vpc_connector_name

        # env and command here
        containers = [self._add_container_settings({"image": self.image})]

        # apply this timeout to each task
        timeout_seconds = str(self.timeout)

        body = {
            "apiVersion": "run.googleapis.com/v1",
            "kind": "Job",
            "metadata": jobs_metadata,
            "spec": {  # JobSpec
                "template": {  # ExecutionTemplateSpec
                    "metadata": {"annotations": annotations},
                    "spec": {  # ExecutionSpec
                        "template": {  # TaskTemplateSpec
                            "spec": {
                                "containers": containers,
                                "timeoutSeconds": timeout_seconds,
                            }  # TaskSpec
                        }
                    },
                }
            },
        }
        return body

    def preview(self) -> str:
        """Generate a preview of the job definition that will be sent to GCP."""
        body = self._jobs_body()
        container_settings = body["spec"]["template"]["spec"]["template"]["spec"][
            "containers"
        ][0]["env"]
        body["spec"]["template"]["spec"]["template"]["spec"]["containers"][0]["env"] = [
            container_setting
            for container_setting in container_settings
            if container_setting["name"] != "PREFECT_API_KEY"
        ]
        return json.dumps(body, indent=2)

    def _watch_job_execution(
        self, client, job_execution: Execution, timeout: int, poll_interval: int = 5
    ):
        """
        Update job_execution status until it is no longer running or timeout is reached.
        """
        t0 = time.time()
        while job_execution.is_running():
            job_execution = Execution.get(
                client=client,
                namespace=job_execution.namespace,
                execution_name=job_execution.name,
            )

            elapsed_time = time.time() - t0
            if timeout is not None and elapsed_time > timeout:
                raise RuntimeError(
                    f"Timed out after {elapsed_time}s while waiting for Cloud Run Job "
                    "execution to complete. Your job may still be running on GCP."
                )

            time.sleep(poll_interval)

        return job_execution

    def _wait_for_job_creation(
        self, client: Resource, timeout: int, poll_interval: int = 5
    ):
        """Give created job time to register."""
        job = Job.get(
            client=client, namespace=self.credentials.project, job_name=self.job_name
        )

        t0 = time.time()
        while not job.is_ready():
            ready_condition = (
                job.ready_condition
                if job.ready_condition
                else "waiting for condition update"
            )
            self.logger.info(
                f"Job is not yet ready... Current condition: {ready_condition}"
            )
            job = Job.get(
                client=client,
                namespace=self.credentials.project,
                job_name=self.job_name,
            )

            elapsed_time = time.time() - t0
            if timeout is not None and elapsed_time > timeout:
                raise RuntimeError(
                    f"Timed out after {elapsed_time}s while waiting for Cloud Run Job "
                    "execution to complete. Your job may still be running on GCP."
                )

            time.sleep(poll_interval)

    def _get_client(self) -> Resource:
        """Get the base client needed for interacting with GCP APIs."""
        # region needed for 'v1' API
        api_endpoint = f"https://{self.region}-run.googleapis.com"
        gcp_creds = self.credentials.get_credentials_from_service_account()
        options = ClientOptions(api_endpoint=api_endpoint)

        return discovery.build(
            "run", "v1", client_options=options, credentials=gcp_creds
        ).namespaces()
