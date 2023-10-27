import json
import re
import time
from typing import Dict, Literal, Optional
from uuid import uuid4

from anyio.abc import TaskStatus
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery, errors
from googleapiclient.discovery import Resource
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import BaseModel, Field

from prefect_gcp.credentials import GcpCredentials


class JobV2(BaseModel):
    """
    JobV2 is a data model for a job that will be run on Cloud Run with the V2 API.
    """

    name: str
    uid: str
    generation: str
    labels: Dict[str, str]
    annotations: Dict[str, str]
    createTime: str
    updateTime: str
    deleteTime: Optional[str]
    expireTime: Optional[str]
    creator: Optional[str]
    lastModifier: Optional[str]
    client: Optional[str]
    clientVersion: Optional[str]
    launchStage: Literal[
        "ALPHA",
        "BETA",
        "GA",
        "DEPRECATED",
        "EARLY_ACCESS",
        "PRELAUNCH",
        "UNIMPLEMENTED",
        "LAUNCH_TAG_UNSPECIFIED",
    ]
    binaryAuthorization: Dict
    template: Dict
    observedGeneration: Optional[str]
    terminalCondition: Dict
    conditions: list[Dict]
    executionCount: int
    latestCreatedExecution: Dict
    reconciling: bool
    satisfiesPzs: bool
    etag: str

    def is_ready(self) -> bool:
        """
        Check if the job is ready to run.

        Returns:
            Whether the job is ready to run.
        """
        ready_condition = self.get_ready_condition()

        if self._is_missing_container(ready_condition=ready_condition):
            raise Exception(f"{ready_condition.get('message')}")

        return ready_condition.get("state") == "CONDITION_SUCCEEDED"

    def get_ready_condition(self) -> Dict:
        """
        Get the ready condition for the job.

        Returns:
            The ready condition for the job.
        """
        if self.terminalCondition.get("type") == "Ready":
            return self.terminalCondition

        return {}

    @classmethod
    def get(
        cls,
        cr_client: Resource,
        project: str,
        location: str,
        job_name: str,
    ):
        """
        Get a job from Cloud Run with the V2 API.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
            project (str): The GCP project ID.
            location (str): The GCP region.
            job_name (str): The name of the job to get.
        """
        # noinspection PyUnresolvedReferences
        request = cr_client.jobs().get(
            name=f"projects/{project}/locations/{location}/jobs/{job_name}",
        )

        response = request.execute()

        return cls(
            name=response["name"],
            uid=response["uid"],
            generation=response["generation"],
            labels=response.get("labels", {}),
            annotations=response.get("annotations", {}),
            createTime=response["createTime"],
            updateTime=response["updateTime"],
            deleteTime=response.get("deleteTime"),
            expireTime=response.get("expireTime"),
            creator=response.get("creator"),
            lastModifier=response.get("lastModifier"),
            client=response.get("client"),
            clientVersion=response.get("clientVersion"),
            launchStage=response.get("launchStage", "GA"),
            binaryAuthorization=response.get("binaryAuthorization", {}),
            template=response.get("template"),
            observedGeneration=response.get("observedGeneration"),
            terminalCondition=response.get("terminalCondition", {}),
            conditions=response.get("conditions", []),
            executionCount=response.get("executionCount", 0),
            latestCreatedExecution=response["latestCreatedExecution"],
            reconciling=response.get("reconciling", False),
            satisfiesPzs=response.get("satisfiesPzs", False),
            etag=response["etag"],
        )

    @staticmethod
    def create(
        cr_client: Resource,
        project: str,
        location: str,
        job_id: str,
        body: Dict,
    ) -> Dict:
        """
        Create a job on Cloud Run with the V2 API.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
            project (str): The GCP project ID.
            location (str): The GCP region.
            job_id (str): The ID of the job to create.
            body (Dict): The job body.
        Returns:
            The response from the Cloud Run V2 API.
        """
        # noinspection PyUnresolvedReferences
        request = cr_client.jobs().create(
            parent=f"projects/{project}/locations/{location}",
            jobId=job_id,
            body=body,
        )

        response = request.execute()

        return response

    @staticmethod
    def delete(
        cr_client: Resource,
        project: str,
        location: str,
        job_name: str,
    ) -> Dict:
        """
        Delete a job on Cloud Run with the V2 API.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
            project (str): The GCP project ID.
            location (str): The GCP region.
            job_name (str): The name of the job to delete.
        Returns:
            Dict: The response from the Cloud Run V2 API.
        """
        # noinspection PyUnresolvedReferences
        list_executions_request = (
            cr_client.jobs()
            .executions()
            .list(
                parent=f"projects/{project}/locations/{location}/jobs/{job_name}",
            )
        )
        list_executions_response = list_executions_request.execute()

        for execution_to_delete in list_executions_response.get("executions", []):
            # noinspection PyUnresolvedReferences
            delete_execution_request = (
                cr_client.jobs()
                .executions()
                .delete(
                    name=execution_to_delete["name"],
                )
            )
            delete_execution_request.execute()

            # Sleep 3 seconds so that the execution is deleted before deleting the job
            time.sleep(3)

        # noinspection PyUnresolvedReferences
        request = cr_client.jobs().delete(
            name=f"projects/{project}/locations/{location}/jobs/{job_name}",
        )

        response = request.execute()

        return response

    @staticmethod
    def run(
        cr_client: Resource,
        project: str,
        location: str,
        job_name: str,
    ):
        """
        Run a job on Cloud Run with the V2 API.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
            project (str): The GCP project ID.
            location (str): The GCP region.
            job_name (str): The name of the job to run.
        """
        # noinspection PyUnresolvedReferences
        request = cr_client.jobs().run(
            name=f"projects/{project}/locations/{location}/jobs/{job_name}",
        )

        response = request.execute()

        return response

    @staticmethod
    def _is_missing_container(ready_condition: Dict) -> bool:
        """
        Check if the job is missing a container.

        Args:
            ready_condition (Dict): The ready condition for the job.
        Returns:
            Whether the job is missing a container.
        """
        if (
            ready_condition.get("state") == "CONTAINER_FAILED"
            and ready_condition.get("reason") == "ContainerMissing"
        ):
            return True

        return False


class ExecutionV2(BaseModel):
    """
    ExecutionV2 is a data model for an execution of a job that will be run on
        Cloud Run API v2.
    """

    name: str
    uid: str
    generation: str
    labels: Dict[str, str]
    annotations: Dict[str, str]
    createTime: str
    startTime: Optional[str]
    completionTime: Optional[str]
    deleteTime: Optional[str]
    expireTime: Optional[str]
    launchStage: Literal[
        "ALPHA",
        "BETA",
        "GA",
        "DEPRECATED",
        "EARLY_ACCESS",
        "PRELAUNCH",
        "UNIMPLEMENTED",
        "LAUNCH_TAGE_UNSPECIFIED",
    ]
    job: str
    parallelism: int
    taskCount: int
    template: Dict
    reconciling: bool
    conditions: list[Dict]
    observedGeneration: Optional[str]
    runningCount: Optional[int]
    succeededCount: Optional[int]
    failedCount: Optional[int]
    cancelledCount: Optional[int]
    retriedCount: Optional[int]
    logUri: str
    satisfiesPzs: bool
    etag: str

    def is_running(self) -> bool:
        """
        Return whether the execution is running.

        Returns:
            Whether the execution is running.
        """
        return self.completionTime is None

    def succeeded(self) -> bool:
        """
        Return whether the execution succeeded.

        Returns:
            Whether the execution succeeded.
        """
        return True if self.condition_after_completion() else False

    def condition_after_completion(self) -> Dict:
        """
        Return the condition after completion.

        Returns:
            The condition after completion.
        """
        if isinstance(self.conditions, list):
            for condition in self.conditions:
                if (
                    condition["state"] == "CONDITION_SUCCEEDED"
                    and condition["type"] == "Completed"
                ):
                    return condition

        return {}

    @classmethod
    def get(
        cls,
        cr_client: Resource,
        execution_id: str,
    ):
        """
        Get an execution from Cloud Run with the V2 API.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
            execution_id (str): The name of the execution to get, in the form of
                projects/{project}/locations/{location}/jobs/{job}/executions
                    /{execution}
        """
        # noinspection PyUnresolvedReferences
        request = cr_client.jobs().executions().get(name=execution_id)

        response = request.execute()

        return cls(
            name=response["name"],
            uid=response["uid"],
            generation=response["generation"],
            labels=response.get("labels", {}),
            annotations=response.get("annotations", {}),
            createTime=response["createTime"],
            startTime=response.get("startTime"),
            completionTime=response.get("completionTime"),
            deleteTime=response.get("deleteTime"),
            expireTime=response.get("expireTime"),
            launchStage=response.get("launchStage", "GA"),
            job=response["job"],
            parallelism=response["parallelism"],
            taskCount=response["taskCount"],
            template=response["template"],
            reconciling=response.get("reconciling", False),
            conditions=response.get("conditions", []),
            observedGeneration=response.get("observedGeneration"),
            runningCount=response.get("runningCount"),
            succeededCount=response.get("succeededCount"),
            failedCount=response.get("failedCount"),
            cancelledCount=response.get("cancelledCount"),
            retriedCount=response.get("retriedCount"),
            logUri=response["logUri"],
            satisfiesPzs=response.get("satisfiesPzs", False),
            etag=response["etag"],
        )


class CloudRunJobV2Result(InfrastructureResult):
    """Result from a Cloud Run Job."""


class CloudRunJobV2(Infrastructure):
    """
    CloudRunJobV2 is a Prefect Infrastructure for running a job on Cloud Run with
        the V2 API.
    """

    _block_type_slug = "cloud-run-job-v2"
    _block_type_name = "GCP Cloud Run Job V2"
    _description = (
        "A Prefect Infrastructure for running a job on Cloud Run with the V2 API."
    )
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/4CD4wwbiIKPkZDt4U3TEuW/c112fe85653da054b6d5334ef662bec4/gcp.png?h=250"  # noqa: E501
    _documentation_url = "https://prefecthq.github.io/prefect-gcp/cloud_run/#prefect_gcp.cloud_run_v2.CloudRunJobV2"  # noqa: E501

    type: Literal["cloud-run-job-v2"] = Field(
        "cloud-run-job-v2",
        description="The slug for this task type. Must be `cloud-run-job-v2`.",
    )

    credentials: GcpCredentials  # cannot be a field; else it shows as JSON in UI
    region: str = Field(
        ...,
        desciption="The region to run the Cloud Run Job V2 in.",
    )
    image: str = Field(
        ...,
        description=(
            "The image to use for a new Cloud Run Job V2. This value must "
            "refer to an image within either Google Container Registry "
            "or Google Artifact Registry, like `gcr.io/<project_name>/<repo>/`."
        ),
    )
    args: Optional[list[str]] = Field(
        default_factory=list,
        description=(
            "The arguments to pass to the Cloud Run Job V2's entrypoint command."
        ),
    )
    env: Dict[str, str] = Field(
        default_factory=Dict,
        description="The environment variables to pass to the Cloud Run Job V2.",
    )
    labels: Dict[str, str] = Field(
        default_factory=Dict,
        description="The labels to pass to the Cloud Run Job V2.",
    )
    keep_job: Optional[bool] = Field(
        default=False,
        title="Keep Job after Completion",
        defscription=(
            "Whether to keep the Cloud Run Job V2 after it is completed. "
            "If False, the Cloud Run Job V2 will be deleted after completion."
        ),
    )
    launch_stage: Literal[
        "ALPHA",
        "BETA",
        "GA",
        "DEPRECATED",
        "EARLY_ACCESS",
        "PRELAUNCH",
        "UNIMPLEMENTED",
        "LAUNCH_TAG_UNSPECIFIED",
    ] = Field(
        "GA",
        description=(
            "The launch stage of the Cloud Run Job V2. "
            "See https://cloud.google.com/run/docs/about-features-categories "
            "for additional details."
        ),
    )
    max_retries: Optional[int] = Field(
        default=0,
        description="The maximum number of times to retry the Cloud Run Job V2. ",
    )
    cpu: Optional[str] = Field(
        default="1",
        title="CPU",
        description=(
            "The amount of CPU allocated to the Cloud Run Job V2. "
            "The int must be valid based on the rules specified at "
            "https://cloud.google.com/run/docs/configuring/cpu#setting-jobs ."
        ),
    )
    memory: Optional[str] = Field(
        default=None,
        title="Memory",
        description=(
            "The memory to allocate to the Cloud Run job along with the units, which"
            "could be: G, Gi, M, Mi."
        ),
        example="512Mi",
        regex=r"^\d+(?:G|Gi|M|Mi)$",
    )
    timeout: Optional[int] = Field(
        default=600,
        gt=0,
        le=86400,
        title="Job Timeout",
        description=(
            "The length of time that Prefect will wait for a Cloud Run Job to "
            "complete before raising an exception (maximum of 86400 seconds, 1 day)."
        ),
    )
    vpc_connector_name: Optional[str] = Field(
        default=None,
        title="VPC Connector Name",
        description="The name of the VPC connector to use for the Cloud Run Job V2.",
    )

    _job_name: str = None
    _execution: Optional[ExecutionV2] = None

    @sync_compatible
    async def run(
        self,
        task_status: TaskStatus | None = None,
    ) -> CloudRunJobV2Result:
        with self._get_client() as cr_client:
            await run_sync_in_worker_thread(
                self._create_job_and_wait_for_registration,
                cr_client=cr_client,
            )

            self._execution = await run_sync_in_worker_thread(
                self._begin_job_execution,
                cr_client=cr_client,
            )

            if task_status:
                task_status.started(self.job_name)

            result = await run_sync_in_worker_thread(
                self._watch_job_execution_and_get_result,
                cr_client=cr_client,
                execution=self._execution,
                poll_interval=5,
            )

            return result

    @sync_compatible
    async def kill(
        self,
        identifier: str,
        grace_seconds: int = 30,
    ):
        raise NotImplementedError

    def preview(self, redact_values: bool = True) -> str:
        """
        Return a preview of the Cloud Run Job V2.

        Args:
            redact_values (bool): Whether to redact values in the preview from the env.

        Returns:
            A preview of the Cloud Run Job V2 .
        """
        body = self._job_body()

        env = body["template"]["template"]["containers"][0]["env"]

        body["template"]["template"]["containers"][0]["env"] = [
            {
                "name": e["name"],
                "value": "REDACTED",
            }
            for e in env
        ]

        return json.dumps(body, indent=4)

    @property
    def job_name(self) -> str:
        """
        Returns the job name, if it does not exist, it creates it.

        Returns:
            The job name.
        """
        if self._job_name is None:
            modified_image_name = (
                "_".join(self.image.split("/")[-2:])
                .replace(
                    ":",
                    "-",
                )
                .replace("_", "-")
            )

            if len(modified_image_name) > 17:
                modified_image_name = modified_image_name[:17]

            self._job_name = f"{modified_image_name}-{uuid4().hex}"

        return self._job_name

    def _begin_job_execution(self, cr_client: Resource) -> ExecutionV2:
        """
        Submit a job run for execution.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
        Returns:
            The execution.
        """
        try:
            self.logger.info(
                f"Submitting Cloud Run Job V2 {self.job_name} for execution..."
            )

            submission = JobV2.run(
                cr_client=cr_client,
                project=self.credentials.project,
                location=self.region,
                job_name=self.job_name,
            )

            job_execution = ExecutionV2.get(
                cr_client=cr_client,
                execution_id=submission["metadata"]["name"],
            )

            command = (
                " ".join(self.command) if self.command else "default container command"
            )

            self.logger.info(
                f"Cloud Run Job V2 {self.job_name} submitted for execution with "
                f"command: {command}"
            )

            return job_execution
        except Exception as exc:
            self._job_run_submission_error(exc=exc)
            raise

    def _job_run_submission_error(self, exc: Exception):
        """
        Creates a formatted error message for the Cloud Run V2 API errors
        """
        # noinspection PyUnresolvedReferences
        if exc.status_code == 404:
            pat1 = r"The requested URL [^ ]+ was not found on this server"

            if re.findall(pat1, str(exc)):
                # noinspection PyUnresolvedReferences
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

    def _watch_job_execution_and_get_result(
        self,
        cr_client: Resource,
        execution: ExecutionV2,
        poll_interval: int,
    ):
        """
        Watch the job execution and get the result.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
            execution (ExecutionV2): The execution to watch.
            poll_interval (int): The number of seconds to wait between polls.
        """
        try:
            execution = self._watch_job_execution(
                cr_client=cr_client,
                execution=execution,
                poll_interval=poll_interval,
            )
        except Exception as exc:
            self.logger.critical(
                f"Encountered an exception while waiting for job run completion - "
                f"{exc}"
            )
            raise

        if execution.succeeded():
            status_code = 0
            self.logger.info(f"Cloud Run Job V2 {self.job_name} succeeded")
        else:
            status_code = 1
            error_mg = execution.condition_after_completion().get("message")
            self.logger.error(f"Cloud Run Job V2 {self.job_name} failed - {error_mg}")

        self.logger.info(f"Job run logs can be found on GCP at: {execution.logUri}")

        if not self.keep_job:
            self.logger.info(
                f"Deleting completed Cloud Run Job {self.job_name!r} from Google Cloud"
                " Run..."
            )

            try:
                JobV2.delete(
                    cr_client=cr_client,
                    project=self.credentials.project,
                    location=self.region,
                    job_name=self.job_name,
                )
            except Exception as exc:
                self.logger.critical(
                    "Received an exception while deleting the Cloud Run Job V2 "
                    f"- {self.job_name} - {exc}"
                )

        return CloudRunJobV2Result(
            identifier=self.job_name,
            status_code=status_code,
        )

    def _watch_job_execution(
        self,
        cr_client: Resource,
        execution: ExecutionV2,
        poll_interval: int,
    ) -> ExecutionV2:
        """
        Update execution status until it is no longer running or timeout is reached.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
            execution (ExecutionV2): The execution to watch.
            poll_interval (int): The number of seconds to wait between polls.
        Returns:
            The execution.
        """
        t0 = time.time()

        while execution.is_running():
            execution = ExecutionV2.get(
                cr_client=cr_client,
                execution_id=execution.name,
            )

            elapsed_time = time.time() - t0

            if elapsed_time > self.timeout:
                raise RuntimeError(
                    f"Timeout of {self.timeout} seconds reached while waiting for"
                    f" Cloud Run Job V2 {self.job_name} to complete."
                )

            time.sleep(poll_interval)

        return execution

    def _create_job_and_wait_for_registration(self, cr_client: Resource):
        """
        Create the Cloud Run Job V2 and wait for it to be registered.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
        """
        try:
            self.logger.info(f"Creating Cloud Run Job V2 {self.job_name}")
            JobV2.create(
                cr_client=cr_client,
                project=self.credentials.project,
                location=self.region,
                job_id=self.job_name,
                body=self._job_body(),
            )
        except errors.HttpError as exc:
            raise self._create_job_error(exc=exc)

        try:
            self._wait_for_job_creation(cr_client=cr_client)
        except Exception as exc:
            self.logger.critical(
                f"Encountered an exception while waiting for job run creation - {exc}"
            )

            if not self.keep_job:
                self.logger.info(
                    f"Deleting Cloud Run Job V2 {self.job_name} from Google Cloud Run"
                )

                try:
                    JobV2.delete(
                        cr_client=cr_client,
                        project=self.credentials.project,
                        location=self.region,
                        job_name=self.job_name,
                    )
                except Exception as exc2:
                    self.logger.critical(
                        "Received an exception while deleting the Cloud Run Job V2 "
                        f"- {self.job_name} - {exc2}"
                    )

            raise

    def _get_client(self) -> Resource:
        """
        Get the base client needed for interacting with GCP Cloud Run V2 API.

        Returns:
            The base client needed for interacting with GCP Cloud Run V2 API.
        """
        api_endpoint = "https://run.googleapis.com"
        gcp_creds = self.credentials.get_credentials_from_service_account()

        options = ClientOptions(api_endpoint=api_endpoint)

        return (
            discovery.build(
                "run",
                "v2",
                client_options=options,
                credentials=gcp_creds,
                num_retries=3,  # Set to 3 in case of intermittent/connection issues
            )
            .projects()
            .locations()
        )

    def _wait_for_job_creation(
        self,
        cr_client: Resource,
        poll_interval: int = 5,
    ):
        """
        Wait for the Cloud Run Job V2 to be created.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
            poll_interval (int): The number of seconds to wait between polls.
                Defaults to 5 seconds.
        """
        job = JobV2.get(
            cr_client=cr_client,
            project=self.credentials.project,
            location=self.region,
            job_name=self.job_name,
        )

        t0 = time.time()

        while not job.is_ready():
            if not (ready_condition := job.get_ready_condition()):
                ready_condition = "waiting for condition update"

            self.logger.info(f"Current Job Condition: {ready_condition}")

            job = JobV2.get(
                cr_client=cr_client,
                project=self.credentials.project,
                location=self.region,
                job_name=self.job_name,
            )

            elapsed_time = time.time() - t0

            if elapsed_time > self.timeout:
                raise RuntimeError(
                    f"Timeout of {self.timeout} seconds reached while waiting for"
                    f" Cloud Run Job V2 {self.job_name} to be created."
                )

            time.sleep(poll_interval)

    def _create_job_error(self, exc):
        """
        Creates a formatted error message for the Cloud Run V2 API errors
        """
        if exc.status_code == 404:
            raise RuntimeError(
                f"Failed to find resources at {exc.uri}. Confirm that region"
                f" '{self.region}' is the correct region for your Cloud Run Job and"
                f" that {self.credentials.project} is the correct GCP project. If"
                f" your project ID is not correct, you are using a Credentials block"
                f" with permissions for the wrong project."
            ) from exc

        raise exc

    def _job_body(self) -> Dict:
        """
        Creates a properly formatted job body for the Cloud Run V2 API POST CREATE
        request.

        Returns:
            The job body.
        """
        body = {
            "client": "prefect",
            "launchStage": self.launch_stage,
            "template": {
                "template": {
                    "maxRetries": self.max_retries,
                    "timeout": f"{self.timeout}s",
                    "containers": [
                        {
                            "env": [],
                            "image": self.image,
                            "command": self.command,
                            "args": self.args,
                            "resources": {
                                "limits": {
                                    "cpu": self.cpu,
                                    "memory": self.memory,
                                },
                            },
                        },
                    ],
                }
            },
        }

        if self.labels:
            body["labels"].update(self.labels)

        if self.env:
            body["template"]["template"]["containers"][0]["env"] = [self.env]

        return body
