import datetime
import time
from typing import Dict, Optional, Tuple
from uuid import uuid4

from anyio.abc import TaskStatus
from google.api_core.client_options import ClientOptions
from google.cloud.aiplatform.gapic import JobServiceClient
from google.cloud.aiplatform_v1.types.custom_job import (
    ContainerSpec,
    CustomJob,
    CustomJobSpec,
    Scheduling,
    WorkerPoolSpec,
)
from google.cloud.aiplatform_v1.types.job_state import JobState
from google.cloud.aiplatform_v1.types.machine_resources import MachineSpec
from google.protobuf.duration_pb2 import Duration
from prefect.infrastructure import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import Field
from slugify import slugify
from typing_extensions import Literal

from prefect_gcp import GcpCredentials


class VertexAICustomTrainingJobResult(InfrastructureResult):
    """Result from a Vertex AI custom training job."""


class VertexAICustomTrainingJob(Infrastructure):
    """
    Infrastructure block used to run Vertex AI custom training jobs.
    """

    _block_type_name = "Vertex AI Custom Training Job"
    _block_type_slug = "vertex-ai-custom-training-job"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/4CD4wwbiIKPkZDt4U3TEuW/c112fe85653da054b6d5334ef662bec4/gcp.png?h=250"  # noqa

    type: Literal["vertex-ai-custom-training-job"] = Field(
        "vertex-ai-custom-training-job", description="The slug for this task type."
    )

    gcp_credentials: GcpCredentials = Field(
        default_factory=GcpCredentials,
        description=(
            "GCP credentials to use when running the configured "
            "Vertex AI training job."
        ),
    )
    region: str = Field(
        default=...,
        description="The region where the Vertex AI custom training job resides.",
    )
    image: str = Field(
        default=...,
        title="Image Name",
        description=(
            "The image to use for a new Vertex AI custom training job. This value must "
            "refer to an image within either Google Container Registry "
            "or Google Artifact Registry, like `gcr.io/<project_name>/<repo>/`"
        ),
    )
    env: Dict[str, str] = Field(
        default_factory=dict,
        title="Environment Variables",
        description="Environment variables to be passed to your Cloud Run Job.",
    )
    machine_type: str = Field(
        default="n1-standard-4",
        description="The machine type to use for the run, which controls the available "
        "CPU and memory.",
    )
    accelerator_type: Optional[str] = Field(
        default=None, description="The type of accelerator to attach to the machine."
    )
    maximum_run_time: datetime.timedelta = Field(
        default=datetime.timedelta(days=7), description="The maximum job running time."
    )
    network: Optional[str] = Field(
        default=None,
        description="The full name of the Compute Engine network"
        "to which the Job should be peered. Private services access must "
        "already be configured for the network. If left unspecified, the job "
        "is not peered with any network.",
    )
    service_account: Optional[str] = Field(
        default=None,
        description=(
            "Specifies the service account to use "
            "as the run-as account in Vertex AI. The agent submitting jobs must have "
            "act-as permission on this run-as account. If unspecified, the AI "
            "Platform Custom Code Service Agent for the CustomJob's project is "
            "used. Takes precedence over the service account found in gcp_credentials, "
            "and required if a service account cannot be detected in gcp_credentials."
        ),
    )

    job_watch_poll_interval: float = Field(
        default=5.0,
        description=(
            "The amount of time to wait between GCP API calls while monitoring the "
            "state of a Vertex AI Job."
        ),
    )

    @property
    def job_name(self):
        """
        The name can be up to 128 characters long and can be consist of any UTF-8 characters.
        Keeps only alphanumeric characters, dashes, underscores, and periods. Reference:
        https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomJob#google_cloud_aiplatform_CustomJob_display_name
        """  # noqa
        try:
            repo_name = self.image.split("/")[2]  # `gcr.io/<project_name>/<repo>/`"
        except IndexError:
            raise ValueError(
                "The provided image must be from either Google Container Registry "
                "or Google Artifact Registry"
            )

        unique_suffix = uuid4().hex
        job_name = slugify(
            f"{repo_name}-{unique_suffix}",
            regex_pattern=r"[^a-zA-Z0-9-_.]+",
            max_length=128,
        )
        return job_name

    def preview(self) -> str:
        return "not implemented..."

    def _build_job_spec(self) -> CustomJobSpec:
        # gather worker pool spec
        env_list = [{"name": name, "value": value} for name, value in self.env.items()]
        container_spec = ContainerSpec(
            image_uri=self.image, command=self.command, args=[], env=env_list
        )
        machine_spec = MachineSpec(
            machine_type=self.machine_type, accelerator_type=self.accelerator_type
        )
        worker_pool_spec = WorkerPoolSpec(
            container_spec=container_spec, machine_spec=machine_spec, replica_count=1
        )

        # look for service account
        service_account = (
            self.service_account or self.gcp_credentials._service_account_email
        )
        if service_account is None:
            raise ValueError(
                "Could not detect a service_account through gcp_credentials; "
                "please provide a service_account in VertexAICustomTrainingJob"
            )

        # build custom job specs
        timeout = Duration().FromTimedelta(td=self.maximum_run_time)
        scheduling = Scheduling(timeout=timeout)
        job_spec = CustomJobSpec(
            worker_pool_specs=[worker_pool_spec],
            service_account=service_account,
            scheduling=scheduling,
            network=self.network,
        )
        return job_spec

    async def _create_and_begin_job(
        self, job_spec: CustomJobSpec, job_service_client: JobServiceClient
    ) -> CustomJob:
        # create custom job
        project = self.gcp_credentials.project
        resource_name = f"projects/{project}/locations/{self.region}"
        custom_job = CustomJob(display_name=self.job_name, job_spec=job_spec)

        # run job
        self.logger.info(
            f"{self._log_prefix}: Preparing to run command {' '.join(self.command)!r} "
            f"in region {self.region!r} using image {self.image!r}..."
        )

        custom_job_run = await run_sync_in_worker_thread(
            job_service_client.create_custom_job,
            parent=resource_name,
            custom_job=custom_job,
        )
        return custom_job_run

    async def _watch_job_run(
        self,
        full_job_name: str,  # different from self.job_name
        job_service_client: JobServiceClient,
        current_state: JobState,
        until_states: Tuple[JobState],
        timeout: int = None,
    ) -> CustomJob:
        state = JobState.JOB_STATE_UNSPECIFIED
        last_state = current_state
        t0 = time.time()

        while state not in until_states:
            job_run = await run_sync_in_worker_thread(
                job_service_client.get_custom_job,
                name=full_job_name,
            )
            state = job_run.state
            if state != last_state:
                state_label = (
                    state.name.replace("_", " ")
                    .lower()
                    .replace("state", "state is now:")
                )
                # results in "New job state is now: succeeded"
                self.logger.info(f"{self._log_prefix}: New {state_label}")
                last_state = state
            else:
                # Intermittently, the job will not be described. We want to respect the
                # watch timeout though.
                self.logger.debug(f"{self._log_prefix}: Job not found.")

            elapsed_time = time.time() - t0
            if timeout is not None and elapsed_time > timeout:
                raise RuntimeError(
                    f"Timed out after {elapsed_time}s while watching job for states "
                    "{until_states!r}"
                )
            time.sleep(self.job_watch_poll_interval)

        return job_run

    @sync_compatible
    async def run(
        self, task_status: Optional[TaskStatus] = None
    ) -> VertexAICustomTrainingJobResult:
        """
        Run the configured task on VertexAI.

        Args:
            task_status: An optional `TaskStatus` to update when the container starts.

        Returns:
            The `VertexAICustomTrainingJobResult`.
        """
        client_options = ClientOptions(
            api_endpoint=f"{self.region}-aiplatform.googleapis.com"
        )

        job_spec = self._build_job_spec()
        with self.gcp_credentials.get_job_service_client(
            client_options=client_options
        ) as job_service_client:
            job_run = await self._create_and_begin_job(job_spec, job_service_client)

            if task_status:
                task_status.started(self.job_name)

            final_job_run = await self._watch_job_run(
                full_job_name=job_run.name,
                job_service_client=job_service_client,
                current_state=job_run.state,
                until_states=(
                    JobState.JOB_STATE_SUCCEEDED,
                    JobState.JOB_STATE_FAILED,
                    JobState.JOB_STATE_CANCELLED,
                    JobState.JOB_STATE_EXPIRED,
                ),
                timeout=self.maximum_run_time.total_seconds(),
            )

        error_msg = final_job_run.error.message
        if error_msg:
            raise RuntimeError(f"{self._log_prefix}: {error_msg}")

        status_code = final_job_run.state.value
        return VertexAICustomTrainingJobResult(
            identifier=final_job_run.display_name, status_code=status_code
        )

    @property
    def _log_prefix(self) -> str:
        """
        Internal property for generating a prefix for logs where `name` may be null
        """
        if self.name is not None:
            return f"VertexAICustomTrainingJob {self.name!r}"
        else:
            return "VertexAICustomTrainingJob"
