import re
import time
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4

import anyio
import googleapiclient
from anyio.abc import TaskStatus
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from googleapiclient.discovery import Resource
from prefect.docker import get_prefect_image_name
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import Field, validator

from prefect_gcp.cloud_run import Execution, Job
from prefect_gcp.credentials import GcpCredentials


def _get_default_job_body_template() -> Dict[str, Any]:
    return {
        "apiVersion": "run.googleapis.com/v1",
        "kind": "Job",
        "metadata": {
            "name": "{{ name }}",
            "annotations": {
                # See: https://cloud.google.com/run/docs/troubleshooting#launch-stage-validation  # noqa
                "run.googleapis.com/launch-stage": "BETA",
                "run.googleapis.com/vpc-access-connector": "{{ vpc_connector_name }}"
            },
        },
        "spec": {  # JobSpec
            "template": {  # ExecutionTemplateSpec
                "spec": {  # ExecutionSpec
                    "template": {  # TaskTemplateSpec
                        "spec": {  # TaskSpec
                            "containers": [
                                {
                                    "image": "{{ image }}",
                                    "args": "{{ args }}",
                                    "resources": {
                                        "limits": {
                                            "cpu": "{{ cpu }}",
                                            "memory": "{{ memory }}",
                                        },
                                        "requests": {
                                            "cpu": "{{ cpu }}",
                                            "memory": "{{ memory }}",
                                        }
                                    }
                                }
                            ],
                            "timeoutSeconds": "{{ timeout }}",
                            "serviceAccountName": "{{ service_account_name }}",
                        }
                    },
                },
            },
        },
    }


class CloudRunWorkerJobConfiguration(BaseJobConfiguration):
    region: str = Field(..., description="The region where the Cloud Run Job resides.")
    credentials: GcpCredentials  # cannot be Field; else it shows as Json
    job_body: Dict[str, Any] = Field(template=_get_default_job_body_template())
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
    keep_job: Optional[bool] = Field(
        default=False,
        title="Keep Job After Completion",
        description="Keep the completed Cloud Run Job on Google Cloud Platform.",
    )

    @property
    def project(self) -> str:
        return self.credentials.project

    @property
    def job_name(self) -> str:
        return self.job_body["metadata"]["name"]

    def prepare_for_flow_run(
        self,
        flow_run: "FlowRun",
        deployment: Optional["DeploymentResponse"] = None,
        flow: Optional["Flow"] = None,
    ):
        """
        Prepares the job configuration for a flow run.

        Ensures that necessary values are present in the job body and that the
        job body is valid.

        Args:
            flow_run: The flow run to prepare the job configuration for
            deployment: The deployment associated with the flow run used for
                preparation.
            flow: The flow associated with the flow run used for preparation.
        """
        super().prepare_for_flow_run(flow_run, deployment, flow)
        self._populate_envs()
        self._populate_service_account_if_not_present()
        self._populate_image_if_not_present()
        self._populate_command_if_not_present()
        self._populate_name_if_not_present()

    def _populate_service_account_if_not_present(self):
        """Populate the service account used for Cloud Run Job Execution if not provided.
        The service account will be the one from `credentials`."""

        try:
            if "serviceAccountName" not in self.job_body["spec"]["template"]["spec"]["template"]["spec"]:
                self.job_body["spec"]["template"]["spec"]["template"]["spec"][
                    "serviceAccountName"
                ] = self.credentials.client_email
        except KeyError:
            raise ValueError(
                "Unable to verify service account due to invalid job body template."
            )

    def _populate_envs(self):
        """Populate environment variables. BaseWorker.prepare_for_flow_run handles
        putting the environment variables in the `env` attribute. This method
        moves them into the jobs body"""
        envs = [{"name": k, "value": v} for k, v in self.env.items()]
        self.job_body["spec"]["template"]["spec"]["template"]["spec"]["containers"][0][
            "env"
        ] = envs

    def _populate_name_if_not_present(self):
        # TODO should we give this a flow run id?
        try:
            if "name" not in self.job_body["metadata"]:
                self.job_body["metadata"]["name"] = f"prefect-job-{uuid4()}"
        except KeyError:
            raise ValueError("Unable to verify name due to invalid job body template.")

    def _populate_image_if_not_present(self):
        try:
            if (
                "image"
                not in self.job_body["spec"]["template"]["spec"]["template"]["spec"][
                    "containers"
                ][0]
            ):
                self.job_body["spec"]["template"]["spec"]["template"]["spec"][
                    "containers"
                ][0]["image"] = f"docker.io/{get_prefect_image_name()}"
        except KeyError:
            raise ValueError("Unable to verify image due to invalid job body template.")

    def _populate_command_if_not_present(self):
        """
        Ensures that the command is present in the job manifest. Populates the command
        with the `prefect -m prefect.engine` if a command is not present.
        """
        try:
            command = self.job_body["spec"]["template"]["spec"]["template"]["spec"][
                "containers"
            ][0].get("args")
            if command is None:
                self.job_body["spec"]["template"]["spec"]["template"]["spec"][
                    "containers"
                ][0]["args"] = [
                    "python",
                    "-m",
                    "prefect.engine",
                ]
            elif isinstance(command, str):
                self.job_body["spec"]["template"]["spec"]["template"]["spec"][
                    "containers"
                ][0]["args"] = command.split()
            elif not isinstance(command, list):
                raise ValueError(
                    "Invalid job manifest template: 'command' must be a string or list."
                )
        except KeyError:
            raise ValueError(
                "Unable to verify command due to invalid job manifest template."
            )


class CloudRunWorkerVariables(BaseVariables):
    region: str = Field(..., description="The region where the Cloud Run Job resides.")
    credentials: GcpCredentials  # cannot be Field; else it shows as Json
    image: Optional[str] = Field(
        default=None,
        title="Image Name",
        description=(
            "The full image to use for a new Cloud Run Job. See https://cloud.google.com/run/docs/deploying#images"
            "for supported image registries. If not set, the latest Prefect image will be used."
        ),
        example="docker.io/prefecthq/prefect:2-latest",
    )
    cpu: Optional[str] = Field(
        default=None,
        title="CPU",
        description=(
            "The amount of compute allocated to the Cloud Run Job. "
            "The int must be valid based on the rules specified at "
            "https://cloud.google.com/run/docs/configuring/cpu#setting-jobs."
            "1000m = 1 CPU."
        ),
        example="1000m",
        regex="^(\d*000)m$"
    )
    memory: Optional[str] = Field(
        default=None,
        title="Memory",
        description="The amount of memory allocated to the Cloud Run Job. See"
                    "https://cloud.google.com/run/docs/configuring/memory-limits#setting "
                    "for additional details. Must end in 'G', 'Gi', 'M', or 'Mi'.",
        example="512Mi",
        regex=r'^\d+(?:G|Gi|M|Mi)$'
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

class CloudRunWorkerResult(BaseWorkerResult):
    """Contains information about the final state of a completed process"""


class CloudRunWorker(BaseWorker):
    type = "cloud-run"
    job_configuration = CloudRunWorkerJobConfiguration
    job_configuration_variables = CloudRunWorkerVariables

    # TODO: remove! this is only to use remote storage for testing convience
    async def _check_flow_run(self, flow_run: "FlowRun") -> None:
        pass

    def _create_job_error(self, exc, configuration):
        """Provides a nicer error for 404s when trying to create a Cloud Run Job."""
        # TODO consider lookup table instead of the if/else,
        # also check for documented errors
        if exc.status_code == 404:
            raise RuntimeError(
                f"Failed to find resources at {exc.uri}. Confirm that region"
                f" '{self.region}' is the correct region for your Cloud Run Job and"
                f" that {configuration.project} is the correct GCP project. If"
                f" your project ID is not correct, you are using a Credentials block"
                f" with permissions for the wrong project."
            ) from exc
        raise exc

    def _job_run_submission_error(self, exc, configuration):
        """Provides a nicer error for 404s when submitting job runs."""
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
                    f"and that '{configuration.project}' is the "
                    f"correct GCP project. If your project ID is not "
                    f"correct, you are using a Credentials "
                    f"block with permissions for the wrong project."
                ) from exc
            else:
                raise exc

        raise exc

    @sync_compatible
    async def run(
        self,
        flow_run: "FlowRun",
        configuration: CloudRunWorkerJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> BaseWorkerResult:
        with self._get_client(configuration) as client:
            await run_sync_in_worker_thread(
                self._create_job_and_wait_for_registration, configuration, client
            )
            job_execution = await run_sync_in_worker_thread(
                self._begin_job_execution, configuration, client
            )

            if task_status:
                task_status.started(configuration.job_name)

            result = await run_sync_in_worker_thread(
                self._watch_job_execution_and_get_result,
                configuration,
                client,
                job_execution,
            )
            return result

    def _get_client(self, configuration: CloudRunWorkerJobConfiguration) -> Resource:
        """Get the base client needed for interacting with GCP APIs."""
        # region needed for 'v1' API
        api_endpoint = f"https://{configuration.region}-run.googleapis.com"
        gcp_creds = configuration.credentials.get_credentials_from_service_account()
        options = ClientOptions(api_endpoint=api_endpoint)

        return discovery.build(
            "run", "v1", client_options=options, credentials=gcp_creds
        ).namespaces()

    def _create_job_and_wait_for_registration(
        self, configuration: CloudRunWorkerJobConfiguration, client: Resource
    ) -> None:
        """Create a new job wait for it to finish registering."""
        try:
            self._logger.info(f"Creating Cloud Run Job {configuration.job_name}")
            import pprint
            pprint.pprint(configuration.job_body)
            Job.create(
                client=client,
                namespace=configuration.credentials.project,
                body=configuration.job_body,
            )
        except googleapiclient.errors.HttpError as exc:
            self._create_job_error(exc, configuration)

        try:
            self._wait_for_job_creation(client=client, configuration=configuration)
        except Exception:
            self._logger.exception(
                "Encountered an exception while waiting for job run creation"
            )
            if not configuration.keep_job:
                self._logger.info(
                    f"Deleting Cloud Run Job {configuration.job_name} from Google Cloud Run."
                )
                try:
                    Job.delete(
                        client=client,
                        namespace=configuration.credentials.project,
                        job_name=configuration.job_name,
                    )
                except Exception:
                    self._logger.exception(
                        "Received an unexpected exception while attempting to delete"
                        f" Cloud Run Job {configuration.job_name!r}"
                    )
            raise

    def _begin_job_execution(
        self, configuration: CloudRunWorkerJobConfiguration, client: Resource
    ) -> Execution:
        """Submit a job run for execution and return the execution object."""
        try:
            self._logger.info(
                f"Submitting Cloud Run Job {configuration.job_name!r} for execution."
            )
            submission = Job.run(
                client=client,
                namespace=configuration.project,
                job_name=configuration.job_name,
            )

            job_execution = Execution.get(
                client=client,
                namespace=submission["metadata"]["namespace"],
                execution_name=submission["metadata"]["name"],
            )
        except Exception as exc:
            self._job_run_submission_error(exc, configuration)

        return job_execution

    def _watch_job_execution_and_get_result(
        self,
        configuration: CloudRunWorkerJobConfiguration,
        client: Resource,
        execution: Execution,
        poll_interval: int = 5,
    ) -> CloudRunWorkerResult:
        """Wait for execution to complete and then return result."""
        try:
            job_execution = self._watch_job_execution(
                client=client,
                job_execution=execution,
                timeout=configuration.timeout,
                poll_interval=poll_interval,
            )
        except Exception:
            self._logger.exception(
                "Received an unexpected exception while monitoring Cloud Run Job "
                f"{configuration.job_name!r}"
            )
            raise

        if job_execution.succeeded():
            status_code = 0
            self._logger.info(
                f"Job Run {configuration.job_name} completed successfully"
            )
        else:
            status_code = 1
            error_msg = job_execution.condition_after_completion()["message"]
            self._logger.error(
                f"Job Run {configuration.job_name} did not complete successfully. {error_msg}"
            )

        self._logger.info(
            f"Job Run logs can be found on GCP at: {job_execution.log_uri}"
        )

        if not configuration.keep_job:
            self._logger.info(
                f"Deleting completed Cloud Run Job {configuration.job_name!r} from Google Cloud"
                " Run..."
            )
            try:
                Job.delete(
                    client=client,
                    namespace=configuration.project,
                    job_name=configuration.job_name,
                )
            except Exception:
                self._logger.exception(
                    "Received an unexpected exception while attempting to delete Cloud"
                    f" Run Job {configuration.job_name}"
                )

        return CloudRunWorkerResult(
            identifier=configuration.job_name, status_code=status_code
        )

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
        self,
        client: Resource,
        configuration: CloudRunWorkerJobConfiguration,
        poll_interval: int = 5,
    ):
        """Give created job time to register."""
        job = Job.get(
            client=client,
            namespace=configuration.project,
            job_name=configuration.job_name,
        )

        t0 = time.time()
        while not job.is_ready():
            ready_condition = (
                job.ready_condition
                if job.ready_condition
                else "waiting for condition update"
            )
            self._logger.info(
                f"Job is not yet ready... Current condition: {ready_condition}"
            )
            job = Job.get(
                client=client,
                namespace=configuration.project,
                job_name=configuration.job_name,
            )

            elapsed_time = time.time() - t0
            if (
                configuration.timeout is not None
                and elapsed_time > configuration.timeout
            ):
                raise RuntimeError(
                    f"Timed out after {elapsed_time}s while waiting for Cloud Run Job "
                    "execution to complete. Your job may still be running on GCP."
                )

            time.sleep(poll_interval)
