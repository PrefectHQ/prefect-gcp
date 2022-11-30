import datetime
from typing import Dict, Optional
from uuid import uuid4

from anyio.abc import TaskStatus
from google.api_core.client_options import ClientOptions
from google.cloud.aiplatform_v1.types.custom_job import (
    CustomJob,
    CustomJobSpec,
    Scheduling,
    WorkerPoolSpec,
)
from google.cloud.aiplatform_v1.types.machine_resources import MachineSpec
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
    machine_type: Optional[str] = Field(
        default=None,
        description="The machine type to use for the run, which controls the available "
        "CPU and memory.",
    )
    accelerator_type: Optional[str] = Field(
        default=None, description="The type of accelerator to attach to the machine."
    )
    maximum_run_time: Optional[datetime.timedelta] = Field(
        default=None, description="The maximum job running time. The default is 7 days."
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
        # instantiate client
        client_options = ClientOptions(
            api_endpoint=f"{self.region}-aiplatform.googleapis.com"
        )
        job_service_client = self.gcp_credentials.get_job_service_client(
            client_options=client_options
        )

        # gather worker pool spec
        machine_spec = MachineSpec(
            machine_type=self.machine_type, accelerator_type=self.accelerator_type
        )
        worker_pool_spec = WorkerPoolSpec(machine_spec=machine_spec, replica_count=1)

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
        scheduling = Scheduling(timeout=self.maximum_run_time)
        job_spec = CustomJobSpec(
            worker_pool_specs=[worker_pool_spec],
            service_account=service_account,
            scheduling=scheduling,
            network=self.network,
        )

        # create custom job
        project = self.gcp_credentials.project
        resource_name = f"projects/{project}/locations/{self.region}"
        custom_job = CustomJob(display_name=self.job_name, job_spec=job_spec)

        # run job
        self.logger.info(
            f"{self._log_prefix}: Preparing to run command {' '.join(self.command)!r} "
            f"in region {self.region!r} using image {self.image!r}..."
        )
        response = await run_sync_in_worker_thread(
            job_service_client.create_custom_job,
            parent=resource_name,
            custom_job=custom_job,
        )

        return VertexAICustomTrainingJobResult(
            identifier=response.name, status_code=response.state.value
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
