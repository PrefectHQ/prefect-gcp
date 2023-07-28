import json
import re
import time
from typing import Any, Optional
from uuid import uuid4

import googleapiclient
from anyio.abc import TaskStatus
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from googleapiclient.discovery import Resource
from prefect.exceptions import InfrastructureNotFound
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import BaseModel, Field, root_validator, validator
from typing_extensions import Literal

from prefect_gcp.credentials import GcpCredentials


class JobV2(BaseModel):
    """
    Utility clas to call GCP `jobs` API (v2) and interact with the returned objects
    """

    name: str
    uid: str
    generation: str
    labels: Optional[dict]
    annotations: Optional[dict]
    create_time: str
    update_time: str
    delete_time: Optional[str]
    expire_time: Optional[str]
    creator: Optional[str]
    last_modifier: Optional[str]
    client: Optional[str]
    client_version: Optional[str]
    launch_stage: str
    binary_authorization: Optional[dict]
    template: dict
    observed_generation: Optional[str]
    terminal_condition: dict
    conditions: Optional[list]
    execution_count: Optional[int]
    latest_created_execution: dict
    reconciling: Optional[bool]
    satisfies_pzs: Optional[bool]
    etag: str
    ready_condition: dict
    execution_status: dict

    def is_ready(self) -> bool:
        """
        Whether a job is finished registering and ready to be executed
        """
        if self._is_missing_container():
            raise Exception(f"{self.ready_condition['message']}")
        return self.ready_condition.get("state") == "CONDITION_SUCCEEDED"

    def _is_missing_container(self):
        """
        Check if Job status is not ready because
        the specified container cannot be found.
        """
        if (
            self.ready_condition.get("state") == "CONTAINER_FAILED"
            and self.ready_condition.get("reason") == "CONTAINER_MISSING"
        ):
            return True
        return False

    @staticmethod
    def _get_ready_condition(job: dict) -> dict:
        """
        Utility to access JSON field containing ready condition.
        """
        terminal_condition = job.get("terminalCondition")

        if terminal_condition:
            if terminal_condition.get("type") == "Ready":
                return terminal_condition

        return {}

    @staticmethod
    def _get_execution_status(job: dict):
        """
        Utility to access JSON field containing execution status.
        """
        if job.get("latestCreatedExecution"):
            return job["latestCreatedExecution"]

        return {}

    @classmethod
    def get(
        cls,
        client: Resource,
        project: str,
        location: str,
        job_name: str,
    ):
        """
        Makes a GET request to the GCP jobs API (v2) and returns a Job Instance)
        """
        request = client.jobs().get(
            name=f"projects/{project}/locations/{location}/jobs/{job_name}",
        )

        response = request.execute()

        return cls(
            name=response["name"],
            uid=response["uid"],
            generation=response["generation"],
            labels=response.get("labels"),
            annotations=response.get("annotations"),
            create_time=response["createTime"],
            update_time=response["updateTime"],
            delete_time=response.get("deleteTime"),
            expire_time=response.get("expireTime"),
            creator=response.get("creator"),
            last_modifier=response.get("lastModifier"),
            client=response.get("client"),
            client_version=response.get("clientVersion"),
            launch_stage=response["launchStage"],
            binary_authorization=response.get("binaryAuthorization"),
            template=response.get("template"),
            observed_generation=response.get("observedGeneration"),
            terminal_condition=response.get("terminalCondition"),
            conditions=response.get("conditions"),
            execution_count=response.get("executionCount", 0),
            latest_created_execution=response.get("latestCreatedExecution", {}),
            reconciling=response.get("reconciling"),
            satisfies_pzs=response.get("satisfiesPzs"),
            etag=response.get("etag"),
            ready_condition=cls._get_ready_condition(response),
            execution_status=cls._get_execution_status(response),
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
        Make a create request to the GCP jobs API (v2).
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
        Make a delete request to the GCP jobs API (v2).
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
        Make a run request to the GCP jobs API.
        """
        request = client.jobs().run(
            name=f"projects/{project}/locations/{location}/jobs/{job_name}"
        )
        response = request.execute()

        return response


class ExecutionV2(BaseModel):
    """
    Utility class to call GCP `executions` API (v2) and interact with the returned
    objects.
    """

    name: str
    uid: str
    generation: str
    labels: Optional[dict]
    annotations: Optional[dict]
    create_time: str
    start_time: Optional[str]
    completion_time: Optional[str]
    update_time: Optional[str]
    delete_time: Optional[str]
    expire_time: Optional[str]
    launch_stage: str
    job: str
    parallelism: int
    task_count: int
    template: dict
    reconciling: Optional[bool]
    conditions: Optional[list]
    observed_generation: Optional[str]
    running_count: Optional[int]
    succeeded_count: Optional[int]
    failed_count: Optional[int]
    retried_count: Optional[int]
    log_uri: str
    satisfies_pzs: Optional[bool]
    etag: str

    def is_running(self) -> bool:
        """
        Returns True if Execution is not completed.
        """
        return self.completion_time is None

    def condition_after_completion(self) -> dict:
        """
        Returns Execution condition if Execution has completed.
        """
        if isinstance(self.conditions, list) and len(self.conditions):
            for condition in self.conditions:
                if condition["state"] == "CONDITION_SUCCEEDED":
                    return condition

        return {}

    def succeeded(self) -> bool:
        """
        Whether or not the Execution completed is a successful state.
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
        Make a get request to the GCP executions API
        and return an Execution instance.

        Execution Name format:
            ToDo: Fill In
        """
        request = client.jobs().executions().get(name=execution_name)
        response = request.execute()

        return cls(
            name=response["name"],
            uid=response["uid"],
            generation=response["generation"],
            labels=response.get("labels"),
            annotations=response.get("annotations"),
            create_time=response["createTime"],
            start_time=response.get("startTime"),
            completion_time=response.get("completionTime"),
            update_time=response.get("updateTime"),
            delete_time=response.get("deleteTime"),
            expire_time=response.get("expireTime"),
            launch_stage=response["launchStage"],
            job=response["job"],
            parallelism=response["parallelism"],
            task_count=response["taskCount"],
            template=response["template"],
            reconciling=response.get("reconciling"),
            conditions=response.get("conditions"),
            observed_generation=response.get("observedGeneration"),
            running_count=response.get("runningCount"),
            succeeded_count=response.get("succeededCount"),
            failed_count=response.get("failedCount"),
            retried_count=response.get("retriedCount"),
            log_uri=response["logUri"],
            satisfies_pzs=response.get("satisfiesPzs"),
            etag=response["etag"],
        )


class CloudRunJobResultV2(InfrastructureResult):
    """
    Result from a Cloud Run Job.
    """


class CloudRunJobV2(Infrastructure):
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
        "#prefect_gcp.cloud_run.CloudRunJob"
    )  # noqa: E501
    # ToDo: add this

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
            "https://cloud.google.com/run/docs/reference/rest/v2/LaunchStage"
        ),
    )
    binary_authorization: Optional[dict] = Field(
        default={},
        title="Binary Authorization",
        description=(
            "Settings for Binary Authorization feature."
            "https://cloud.google.com/run/docs/reference/rest/v2/BinaryAuthorization"
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
    vpc_connector: Optional[dict] = Field(
        default={},
        title="VPC Access",
        description=(
            "VPC Access settings. For more information on creating a VPC Connector, "
            "visit https://cloud.google.com/vpc/docs/configure-serverless-vpc-access "
            "For information on how to configure Cloud Run with an existing VPC "
            "Connector, visit "
            "https://cloud.google.com/run/docs/configuring/connecting-vpc"
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

    # for private use
    _job_name: str = None
    _execution: Optional[ExecutionV2] = None

    @property
    def job_name(self) -> str:
        """
        Create a unique and valid job name.
        """

        if self._job_name is None:
            # get `repo` from `gcr.io/<project_name>/repo/other`
            components = self.image.split("/")
            image_name = components[2]
            # only alphanumeric and '-' allowed for a job name
            modified_image_name = image_name.replace(":", "-").replace(".", "-")
            # make 50 char limit for final job name, which will be '<name>-<uuid>'
            if len(modified_image_name) > 17:
                modified_image_name = modified_image_name[:17]
            name = f"{modified_image_name}-{uuid4().hex}"
            self._job_name = name

        return self._job_name

    @property
    def memory_string(self) -> str | None:
        """
        Returns the string expected for memory resources argument.
        """
        if self.memory and self.memory_unit:
            return str(self.memory) + self.memory_unit
        return None

    @validator("image")
    def _remove_image_spaces(cls, value) -> str | None:
        """
        Deal with spaces in image names.
        """
        if value is not None:
            return value.strip()

    @root_validator
    def _check_valid_memory(cls, values) -> dict:
        """
        Make sure memory conforms to expected values for API.
        See: https://cloud.google.com/run/docs/configuring/memory-limits#setting
        """
        if (values.get("memory") is not None and values.get("memory_unit") is None) or (
            values.get("memory_unit") is not None and values.get("memory") is None
        ):
            raise ValueError(
                "A memory value and unit must both be supplied to specify a memory"
                " value other than the default memory value."
            )
        return values

    def _create_job_error(self, exc):
        """
        Provides a nicer error for 404s when trying to create a Cloud Run Job.
        """
        # TODO consider lookup table instead of the if/else,
        # also check for documented errors
        if exc.status_code == 404:
            raise RuntimeError(
                f"Failed to find resources at {exc.uri}. Confirm that region"
                f" '{self.region}' is the correct region for your Cloud Run Job and"
                f" that {self.credentials.project} is the correct GCP project. If"
                f" your project ID is not correct, you are using a Credentials block"
                f" with permissions for the wrong project."
            ) from exc
        raise exc

    def _job_run_submission_error(self, exc):
        """
        Provides a nicer error for 404s when submitting job runs.
        """
        if exc.status_code == 404:
            pat1 = r"The requested URL [^ ]+ was not found on this server"
            # pat2 = (
            #     r"Resource '[^ ]+' of kind 'JOB' in region '[\w\-0-9]+' "
            #     r"in project '[\w\-0-9]+' does not exist"
            # )
            if re.findall(pat1, str(exc)):
                raise RuntimeError(
                    f"Failed to find resources at {exc.uri}. "
                    f"Confirm that region '{self.region}' is "
                    f"the correct region for your Cloud Run Job "
                    f"and that '{self.credentials.project}' is the "
                    f"correct GCP project. If your project ID is not "
                    f"correct, you are using a Credentials "
                    f"block with permissions for the wrong project."
                ) from exc
            else:
                raise exc

        raise exc

    def _cpu_as_k8s_quantity(self) -> str:
        """
        Return the CPU integer in the format expected by GCP Cloud Run Jobs API.
        See:
         https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
        See also: https://cloud.google.com/run/docs/configuring/cpu#setting-jobs
        """
        return str(self.cpu * 1000) + "m"

    @sync_compatible
    async def run(self, task_status: Optional[TaskStatus] = None) -> Any:  # ToDo: Any?
        """
        Run the configured job on a Google Cloud Run Job.
        """
        with self._get_client() as client:
            await run_sync_in_worker_thread(
                self._create_job_and_wait_for_registration, client
            )
            job_execution = await run_sync_in_worker_thread(
                self._begin_job_execution, client
            )

            if task_status:
                task_status.started(self.job_name)

            result = await run_sync_in_worker_thread(
                self._watch_job_execution_and_get_result,
                client,
                job_execution,
                5,
            )
            return result

    @sync_compatible
    async def kill(self, identifier: str, grace_seconds: int = 30) -> None:
        """
        Kill a task running Cloud Run.

        Args:
            identifier: The Cloud Run Job name. This should match a
                value yielded by CloudRunJob.run.
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

    def _kill_job(self, client: Resource, job_name: str) -> None:
        """
        Thin wrapper around Job.delete, wrapping a try/except since
        Job is an independent class that doesn't have knowledge of
        CloudRunJob and its associated logic.
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

    def _create_job_and_wait_for_registration(self, client: Resource) -> None:
        """
        Create a new job wait for it to finish registering.
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
        Submit a job run for execution and return the execution object.
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

    def _jobs_body(self) -> dict:
        """
        Create properly formatted body used for a Job CREATE request.
        See: https://cloud.google.com/run/docs/reference/rest/v2/
            projects.locations.jobs/create
        """
        annotations = {}

        # env and command here
        containers = [self._add_container_settings({"image": self.image})]

        # apply this timeout to each task
        timeout_seconds = f"{self.timeout}s"

        body = {
            "labels": self.labels,
            "annotations": annotations,
            "launchStage": self.launch_stage,
            "binaryAuthorization": self.binary_authorization,
            "template": {
                "labels": self.labels,
                "annotations": annotations,
                "parallelism": self.parallelism,
                "task_count": self.task_count,
                "template": {
                    "containers": containers,
                    "timeout": timeout_seconds,
                },
            },
        }

        return body

    def preview(self) -> str:
        """
        Generate a preview of the job definition that will be sent to GCP.
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
        """Give created job time to register."""
        job = JobV2.get(
            client=client,
            project=self.credentials.project,
            location=self.region,
            job_name=self.job_name,
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

    # CONTAINER SETTINGS
    def _add_container_settings(
        self,
        base_settings: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Add settings related to containers for Cloud Run Jobs to a dictionary.
        Includes environment variables, entrypoint command, entrypoint arguments,
        and cpu and memory limits.
        """
        container_settings = base_settings.copy()
        container_settings.update(self._add_env())
        container_settings.update(self._add_resources())
        container_settings.update(self._add_command())
        container_settings.update(self._add_args())

        return container_settings

    def _add_args(self) -> dict:
        """
        Set the arguments that will be passed to the entrypoint for a Cloud Run Job.
        """
        return {"args": self.args} if self.args else {}

    def _add_command(self) -> dict:
        """
        Set the command that a container will run for a Cloud Run Job.
        """
        return {"command": self.command}

    def _add_resources(self) -> dict:
        """
        Set specified resources limits for a Cloud Run Job.
        """
        resources = {"limits": {}}

        if self.cpu is not None:
            cpu = self._cpu_as_k8s_quantity()
            resources["limits"]["cpu"] = cpu
        if self.memory_string is not None:
            resources["limits"]["memory"] = self.memory_string

        return {"resources": resources}

    def _add_env(self) -> dict:
        """
        Add environment variables for a Cloud Run Job.

        Method `self._base_environment()` gets necessary Prefect environment variables
        from the config.
        """
        env = {**self._base_environment(), **self.env}
        cloud_run_env = [{"name": k, "value": v} for k, v in env.items()]
        return {"env": cloud_run_env}

    @staticmethod
    def _watch_job_execution(
        client,
        job_execution: ExecutionV2,
        timeout: int,
        poll_interval: int = 5,
    ):
        """
        Update job_execution status until it is no longer running or timeout is reached.
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
