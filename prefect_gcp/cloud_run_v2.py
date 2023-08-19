import json
import time
from typing import Dict, List, Optional

import googleapiclient
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from googleapiclient.discovery import Resource
from prefect.exceptions import InfrastructureNotFound
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import Field
from typing_extensions import Literal

from prefect_gcp.base_cloud_run import (
    BaseCloudRunJob,
    BaseCloudRunJobResult,
    BaseExecution,
    BaseJob,
)
from prefect_gcp.credentials import GcpCredentials


class JobV2(BaseJob):
    """
    Jobs for Cloud Run API v2:
    https://cloud.google.com/run/docs/reference/rpc/google.cloud.run.v2#google.cloud.run.v2.Job
    """

    name: Optional[str]
    uid: Optional[str]  # output only
    generation: Optional[str]  # output only
    annotations: Optional[Dict]
    create_time: Optional[str]  # output only
    update_time: Optional[str]  # output only
    creator: Optional[str]  # output only
    last_modifier: Optional[str]  # output only
    client: Optional[str]
    client_version: Optional[str]
    launch_stage: Optional[str]
    binary_authorization: Optional[Dict]
    template: Dict
    observed_generation: Optional[str]  # output only
    terminal_condition: Optional[Dict]  # output only
    conditions: Optional[List]  # output only
    execution_count: Optional[int]  # output only
    latest_created_execution: Optional[Dict]  # output only
    reconciling: Optional[bool]  # output only
    satisfies_pzs: Optional[bool]  # output only
    etag: Optional[str]  # output only

    def is_ready(self) -> bool:
        """
        Reports whether a job is finished registering and ready to be executed.

        Returns:
            A boolean indicating when the Job is `ready` or not.
        """
        ready_condition = self.get_ready_condition()

        if self._is_missing_container():
            raise Exception(f"{ready_condition['message']}")
        return ready_condition.get("state") == "CONDITION_SUCCEEDED"

    def _is_missing_container(self) -> bool:
        """
        Check if Job status is not ready because the specified container cannot
        be found.

        Returns:
            A boolean indicating if the container can't be found/is missing.
        """
        ready_condition = self.get_ready_condition()

        if (
            ready_condition.get("state") == "CONTAINER_FAILED"
            and ready_condition.get("reason") == "CONTAINER_MISSING"
        ):
            return True
        return False

    def has_execution_in_progress(self) -> bool:
        """
        See if job has an execution in progress.

        Returns:
            A boolean indicating if a job has an execution in progress.
        """
        execution_status = self._get_execution_status()

        return (
            execution_status == {}
            or execution_status.get("completionTime") is None
            or execution_status.get("completionTime") == "1970-01-01T00:00:00Z"
        )

    def get_ready_condition(self) -> Dict:
        """
        A utility to access JSON field containing ready condition.

        Returns:
            An empty dictionary if the ready condition is not available, otherwise
            it returns the ready condition.
        """
        terminal_condition = self.terminal_condition

        if terminal_condition:
            if terminal_condition.get("type") == "Ready":
                return terminal_condition

        return {}

    def _get_execution_status(self) -> Dict:
        """
        A utility to access JSON field containing execution status.

        Returns:
            An empty dictionary if the `latest_created_execution is None/unavailable,
            otherwise it returns the `latest_created_execution` dictionary.
        """
        latest_created_execution = self.latest_created_execution

        if latest_created_execution:
            return latest_created_execution

        return {}

    @classmethod
    def get(
        cls,
        client: Resource,
        job_name: str,
        project: str,
        location: str,
    ):
        """
        Makes a GET request to the GCP jobs API to fetch a instance of a specified Job.

        Args:
            client: The GCP Cloud Run API client.
            job_name: The Job's name, used in looking up the job.
            project: The GCP project ID.
            location: The GCP Cloud Run location that you are working in.

        Returns:
            The specified job's instance, if it exists.
        """
        request = client.jobs().get(
            name=f"projects/{project}/locations/{location}/jobs/{job_name}",
        )

        response = request.execute()

        return cls(
            name=response.get("name"),
            uid=response.get("uid"),
            generation=response.get("generation"),
            annotations=response.get("annotations"),
            create_time=response.get("createTime"),
            update_time=response.get("updateTime"),
            creator=response.get("creator"),
            last_modifier=response.get("lastModifier"),
            client=response.get("client"),
            client_version=response.get("clientVersion"),
            launch_stage=response.get("launchStage"),
            binary_authorization=response.get("binaryAuthorization"),
            template=response["template"],
            observed_generation=response.get("observedGeneration"),
            terminal_condition=response.get("terminalCondition"),
            conditions=response.get("conditions"),
            execution_count=response.get("executionCount"),
            latest_created_execution=response.get("latestCreatedExecution"),
            reconciling=response.get("reconciling"),
            satisfies_pzs=response.get("satisfiesPzs"),
            etag=response.get("etag"),
        )

    @staticmethod
    def create(
        client: Resource,
        project: str,
        location: str,
        job_id: str,
        body: dict,
    ):
        """
        Make a create request to the GCP jobs API.

        Args:
            client: The GCP Cloud Run API client.
            project: The GCP project ID.
            location: The GCP Cloud Run location that you are working in.
            job_id: The ID used in creating the Job.
            body: The dictionary of arguments used in creating the Job.
        """
        request = client.jobs().create(
            parent=f"projects/{project}/locations/{location}",
            jobId=job_id,
            body=body,
        )
        response = request.execute()

        return response

    @staticmethod
    def delete(
        client: Resource,
        project: str,
        location: str,
        job_name: str,
    ):
        """
        Make a delete request to the GCP jobs API.

        Args:
            client: The GCP Cloud Run API client.
            project: The GCP project ID.
            location: The GCP Cloud Run location that you are working in.
            job_name: The Job's name.
        """
        request = client.jobs().delete(
            name=f"projects/{project}/locations/{location}/jobs/{job_name}",
        )
        response = request.execute()

        return response

    @staticmethod
    def run(
        client: Resource,
        project: str,
        location: str,
        job_name: str,
    ):
        """
        Makes a run request to the GCP jobs API.

        Args:
            client: The GCP Cloud Run API client.
            project: The GCP project ID.
            location: The GCP Cloud Run location that you are working in.
            job_name: The Job's name.
        """
        request = client.jobs().run(
            name=f"projects/{project}/locations/{location}/jobs/{job_name}"
        )
        response = request.execute()

        return response


class ExecutionV2(BaseExecution):
    """
    Executions for Cloud Run API v2:
    https://cloud.google.com/run/docs/reference/rpc/google.cloud.run.v2#execution
    """

    name: Optional[str]  # output only
    uid: Optional[str]  # output only
    generation: Optional[str]  # output only
    label: Optional[Dict]  # output only
    annotations: Optional[Dict]  # output only
    create_time: Optional[str]  # output only
    start_time: Optional[str]  # output only
    completion_time: Optional[str]  # output only
    update_time: Optional[str]  # output only
    delete_time: Optional[str]  # output only
    expire_time: Optional[str]  # output only
    launch_stage: Optional[str]
    job: Optional[str]  # output only
    parallelism: Optional[int]  # output only
    task_count: Optional[int]  # output only
    template: Optional[Dict]  # output only
    reconciling: Optional[bool]  # output only
    conditions: Optional[List]  # output only
    observed_generation: Optional[str]  # output only
    running_count: Optional[int]  # output only
    failed_count: Optional[int]  # output only
    cancelled_count: Optional[int]  # output only
    retried_count: Optional[int]  # output only
    log_uri: Optional[str]  # output only
    satisfies_pzs: Optional[bool]  # output only
    etag: Optional[str]  # output only

    def is_running(self) -> bool:
        """
        Indicates whether an Execution is still running or not.

        Returns:
            A boolean indicating if Execution is still running or not.
        """
        return self.completion_time is None

    def condition_after_completion(self) -> dict:
        """
        Looks for the Execution condition that indicates if an execution is finished.

        Returns:
            An empty dictionary if the Execution is not completed successfully,
            otherwise it returns the condition that indicates a successful Execution.
        """
        if isinstance(self.conditions, list) and len(self.conditions):
            for condition in self.conditions:
                if (
                    condition["state"] == "CONDITION_SUCCEEDED"
                    and condition["type"] == "Completed"
                ):
                    return condition

        return {}

    def succeeded(self) -> bool:
        """
        Checks whether or not the Execution completed is a successful state.

        Returns:
            A boolean indicating if an Execution has successfully finished or not.
        """
        completed_condition = self.condition_after_completion()

        if completed_condition:
            return True

        return False

    @classmethod
    def get(
        cls,
        client: Resource,
        execution_name: str,
    ):
        """
        Makes a get request to the GCP executions API and return an Execution instance.

        Args:
            client: The GCP Cloud Run API client.
            execution_name: The Execution name, which is in the following format:
            `projects/{project}/locations/{location}/jobs/{job}/executions/{execution}`.
        """
        request = client.jobs().executions().get(name=execution_name)
        response = request.execute()

        return cls(
            name=response.get("name"),
            uid=response.get("uid"),
            generation=response.get("generation"),
            label=response.get("label"),
            annotations=response.get("annotations"),
            create_time=response.get("createTime"),
            start_time=response.get("startTime"),
            completion_time=response.get("completionTime"),
            update_time=response.get("updateTime"),
            delete_time=response.get("deleteTime"),
            expire_time=response.get("expireTime"),
            launch_stage=response.get("launchStage"),
            job=response.get("job"),
            parallelism=response.get("parallelism"),
            task_count=response.get("taskCount"),
            template=response.get("template"),
            reconciling=response.get("reconciling"),
            conditions=response.get("conditions"),
            observed_generation=response.get("observedGeneration"),
            running_count=response.get("runningCount"),
            failed_count=response.get("failedCount"),
            cancelled_count=response.get("cancelledCount"),
            retried_count=response.get("retriedCount"),
            log_uri=response.get("logUri"),
            satisfies_pzs=response.get("satisfiesPzs"),
            etag=response.get("etag"),
        )


class CloudRunJobResultV2(BaseCloudRunJobResult):
    """
    Result from a Cloud Run Job.
    """


class CloudRunJobV2(BaseCloudRunJob):
    """
    <span class="badge-api experimental"/>

    Infrastructure block used to run GCP Cloud Run Jobs (API v2).

    Project name information is provided by the Credentials object, and should always
    be correct as long as the Credentials object is for the correct project.

    Note this block is experimental. The interface may change without notice.
    """

    _block_type_slug = "cloud-run-job-v2"
    _block_type_name = "GCP Cloud Run Job v2"
    _description = (
        "Infrastructure block used to run GCP Cloud Run Jobs (API v2). "
        "Note this block is experimental. The interface may change without notice."
    )
    _logo_url = (
        "https://images.ctfassets.net/gm98wzqotmnx/4CD4wwbiIKPkZDt4U3TEuW/"
        "c112fe85653da054b6d5334ef662bec4/gcp.png?h=250"
    )
    _documentation_url = (
        "https://prefecthq.github.io/prefect-gcp/cloud_run/"
        "#prefect_gcp.cloud_run_v2.CloudRunJobV2"
    )  # noqa: E501

    type: Literal["cloud-run-job-v2"] = Field(
        "cloud-run-job-v2",
        description="The slug for this task type.",
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
    region: str = Field(
        ...,
        description="The region where the Cloud Run Job resides.",
    )
    credentials: GcpCredentials  # cannot be a field; otherwise it shows as JSON

    # job settings
    cpu: Optional[int] = Field(
        default=None,
        title="CPU",
        description=(
            "The amount of compute allocated to the Cloud Run Job. "
            "The int must be valid based on the rules specified at "
            "https://cloud.google.com/run/docs/configuring/cpu#setting-jobs ."
        ),
    )
    launch_stage: Optional[str] = Field(
        default="GA",
        title="Launch Stage",
        description=(
            "The launch stage as defined by Google Cloud Platform Launch Stages."
            "https://cloud.google.com/run/docs/reference/rest/v2/LaunchStage."
        ),
    )
    binary_authorization: Optional[Dict] = Field(
        default={},
        title="Binary Authorization",
        description=(
            "Settings for Binary Authorization feature."
            "https://cloud.google.com/run/docs/reference/rest/v2/BinaryAuthorization."
        ),
    )
    parallelism: Optional[int] = Field(
        default=1,
        title="Parallelism",
        description=(
            "Specifies the maximum desired number of tasks the execution should "
            "run at given time. Must be <= taskCount. When the job is run, if "
            "this field is 0 or unset, the maximum possible value will be used for "
            "that execution. The actual number of tasks running in steady state "
            "will be less than this number when there are fewer tasks waiting to "
            "be completed remaining, i.e. when the work left to do is less than "
            "max parallelism."
        ),
    )
    task_count: Optional[int] = Field(
        default=1,
        title="Task Count",
        description=(
            "Specifies the desired number of tasks the execution should run. "
            "Setting to 1 means that parallelism is limited to 1 and the success "
            "of that task signals the success of the execution. Defaults to 1."
        ),
    )
    max_retries: Optional[int] = Field(
        default=3,
        title="Max Retries",
        description=(
            "Number of retries allowed per Task, before marking this Task failed."
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
        title="VPC Connector",
        description=(
            "VPC Access connector name. Format: projects/{project}/locations/"
            "{location}/connectors/{connector}, where {project} can be project id "
            "or number."
        ),
    )
    args: Optional[list[str]] = Field(
        default=None,
        description=(
            "Arguments to be passed to your Cloud Run Job's entrypoint command."
        ),
    )
    env: dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables to be passed to your Cloud Run Job.",
    )

    # cleanup behavior
    keep_job: Optional[bool] = Field(
        default=False,
        title="Keep Job After Completion",
        description="Keep the completed Cloud Run Job on Google Cloud Platform.",
    )
    timeout: Optional[int] = Field(
        default=600,
        gt=0,
        le=86400,
        title="Job Timeout",
        description=(
            "The length of time that Prefect will wait for a Cloud Run Job to complete "
            "before raising an exception."
        ),
    )
    service_account: Optional[str] = Field(
        default=None,
        description=(
            "Email address of the IAM service account associated with the Task of a "
            "Job. The service account represents the identity of the running "
            "task, and determines what permissions the task has. If not "
            "provided, the task will use the project's default service account."
        ),
    )

    _job_name: str = None
    _execution: Optional[ExecutionV2] = None

    def _add_resources(self) -> Dict:
        """
        Sets specified resources limits for a Cloud Run Job.

        Returns:
            The added resources dictionary.
        """
        resources = {"limits": {}}

        if self.cpu is not None:
            cpu = self._cpu_as_k8s_quantity()
            resources["limits"]["cpu"] = cpu
        if self.memory_string is not None:
            resources["limits"]["memory"] = self.memory_string

        return {"resources": resources} if resources["limits"] else {}

    @sync_compatible
    async def kill(self, identifier: str, grace_seconds: int = 30):
        """
        Kill a task running Cloud Run.

        Args:
            identifier: The Cloud Run Job name. This should match a value yielded
            by CloudRunJob.run.
            grace_seconds: grace seconds
        """
        if grace_seconds != 30:
            self.logger.warning(
                f"Kill grace period of {grace_seconds}s requested, but GCP does not "
                "support dynamic grace period configuration/"  # noqa
            )

        with self._get_client() as client:
            await run_sync_in_worker_thread(
                self._kill_job,
                client=client,
                job_name=identifier,
            )

    def _kill_job(self, client: Resource, job_name: str):
        """
        Thin wrapper around Job.delete, wrapping a try/except since
        Job is an independent class that doesn't have knowledge of
        CloudRunJob and its associated logic.

        Args:
            client: The GCP Cloud Run API client.
            job_name: The Job's name.
        """
        try:
            JobV2.delete(
                client=client,
                project=self.credentials.project,
                location=self.region,
                job_name=job_name,
            )
        except Exception as exc:
            if "does not exist" in str(exc):
                raise InfrastructureNotFound(
                    f"Cannot stop Cloud Run Job; the job name {job_name!r} "
                    "could not be found."
                ) from exc
            raise

    def _create_job_and_wait_for_registration(self, client: Resource):
        """
        Creates a new job wait for it to finish registering.

        Args:
            client: The GCP Cloud Run API client.
        """
        try:
            self.logger.info(f"Creating Cloud Run Job {self.job_name}")
            JobV2.create(
                client=client,
                project=self.credentials.project,
                location=self.region,
                job_id=self.job_name,
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
                    JobV2.delete(
                        client=client,
                        project=self.credentials.project,
                        location=self.region,
                        job_name=self.job_name,
                    )
                except Exception:
                    self.logger.exception(
                        "Received an unexpected exception while attempting to delete"
                        f" Cloud Run Job {self.job_name!r}"
                    )
            raise

    def _begin_job_execution(self, client: Resource) -> ExecutionV2:
        """
        Submits a job run for execution and return the execution object.

        Args:
            client: The GCP Cloud Run API client.
        """
        try:
            self.logger.info(
                f"Submitting Cloud Run Job {self.job_name!r} for execution."
            )
            submission = JobV2.run(
                client=client,
                project=self.credentials.project,
                location=self.region,
                job_name=self.job_name,
            )

            job_execution = ExecutionV2.get(
                client=client,
                execution_name=submission["metadata"]["name"],
            )

            command = (
                " ".join(self.command) if self.command else "default container command"
            )

            self.logger.info(
                f"Cloud Run Job {self.job_name!r}: Running command {command!r}"
            )

            return job_execution
        except Exception as exc:
            self._job_run_submission_error(exc)

    def _watch_job_execution_and_get_result(
        self,
        client: Resource,
        execution: ExecutionV2,
        poll_interval: int,
    ) -> CloudRunJobResultV2:
        """
        Waits for execution to complete and then return result.

        Args:
            client: The GCP Cloud Run API client.
            execution: The Execution object from the Cloud Run API v2.
            poll_interval: The interval in which polling happens (seconds)

        Returns:
            A CloudRunJobResultV2 instance indicating Job Result status.
        """
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
            error_msg = job_execution.condition_after_completion().get("message")
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
                JobV2.delete(
                    client=client,
                    project=self.credentials.project,
                    location=self.region,
                    job_name=self.job_name,
                )
            except Exception:
                self.logger.exception(
                    "Received an unexpected exception while attempting to delete Cloud"
                    f" Run Job {self.job_name}"
                )

        return CloudRunJobResultV2(identifier=self.job_name, status_code=status_code)

    def _jobs_body(self) -> Dict:
        """
        Creates properly formatted body used for a Job create request.
        See: https://cloud.google.com/run/docs/reference/rest/v2/
            projects.locations.jobs/create

        Returns:
            A properly formatted dictionary used in Job create request in the body.
        """
        annotations = {}

        # env and command here
        containers = [self._add_container_settings({"image": self.image})]

        # apply this timeout to each task
        timeout_seconds = f"{self.timeout}s"

        body = {
            "annotations": annotations,
            "launchStage": self.launch_stage,
            "binaryAuthorization": self.binary_authorization,
            "template": {
                "annotations": annotations,
                "parallelism": self.parallelism,
                "task_count": self.task_count,
                "template": {
                    "containers": containers,
                    "timeout": timeout_seconds,
                    "maxRetries": self.max_retries,
                },
            },
        }

        if self.vpc_connector_name:
            body["template"]["template"]["vpcAccess"] = {
                "connector": self.vpc_connector_name,
            }

        if self.service_account:
            body["template"]["template"]["serviceAccount"] = self.service_account

        return body

    def preview(self) -> str:
        """
        Generates a preview of the job definition that will be sent to GCP.

        Returns:
            A stringed (json.dumps) JSON of the previewed dictionary.
        """
        body = self._jobs_body()
        container_settings = body["template"]["template"]["containers"][0]["env"]
        body["template"]["template"]["containers"][0]["env"] = [
            container_setting
            for container_setting in container_settings
            if container_setting["name"] != "PREFECT_API_KEY"
        ]
        return json.dumps(body, indent=2)

    def _wait_for_job_creation(
        self,
        client: Resource,
        timeout: int,
        poll_interval: int = 5,
    ):
        """
        Gives created job time to register.

        Args:
            client: The GCP Cloud Run API client.
            timeout: The maximum runtime for the Job until it times out.
            poll_interval: The number of second between each status poll request.
        """
        job = JobV2.get(
            client=client,
            project=self.credentials.project,
            location=self.region,
            job_name=self.job_name,
        )

        t0 = time.time()
        while not job.is_ready():
            ready_condition = (
                job.get_ready_condition()
                if job.get_ready_condition()
                else "waiting for condition update"
            )
            self.logger.info(
                f"Job is not yet ready... Current condition: {ready_condition}"
            )
            job = JobV2.get(
                client=client,
                project=self.credentials.project,
                location=self.region,
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
        """
        Get the base client needed for interacting with GCP APIs.

        Returns:
            A client for interacting with GCP Cloud Run v2 API.
        """
        api_endpoint = f"https://{self.region}-run.googleapis.com"
        gcp_creds = self.credentials.get_credentials_from_service_account()
        options = ClientOptions(api_endpoint=api_endpoint)

        return (
            discovery.build(
                "run",
                "v2",
                client_options=options,
                credentials=gcp_creds,
            )
            .projects()
            .locations()
        )

    @staticmethod
    def _watch_job_execution(
        client,
        job_execution: ExecutionV2,
        timeout: int,
        poll_interval: int = 5,
    ):
        """
        Updates job_execution status until it is no longer running or timeout is
        reached.
        """
        t0 = time.time()
        while job_execution.is_running():
            job_execution = ExecutionV2.get(
                client=client,
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
