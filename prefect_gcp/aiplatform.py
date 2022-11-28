import datetime
from typing import Dict, Optional

from anyio.abc import TaskStatus
from prefect.infrastructure import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import sync_compatible
from pydantic import Field

from prefect_gcp import GcpCredentials


class VertexAICustomTrainingJobResult(InfrastructureResult):
    """Result from a Vertex AI custom training job."""


class VertexAICustomTrainingJob(Infrastructure):
    """
    Infrastructure block used to run Vertex AI custom training jobs.
    """

    _block_type_name = "Vertex AI Custom Training Job"
    _block_type_slug = "vertex-ai-job"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/4CD4wwbiIKPkZDt4U3TEuW/c112fe85653da054b6d5334ef662bec4/gcp.png?h=250"  # noqa

    credentials: GcpCredentials = Field(
        default_factory=GcpCredentials,
        description="GCP credentials to use when running the configured Vertex AI "
        "training job.",
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
    service_account: Optional[str] = Field(
        default=None,
        description="Specifies the service account to use "
        "as the run-as account in Vertex AI. The agent submitting jobs must have "
        "act-as permission on this run-as account. If unspecified, the AI "
        "Platform Custom Code Service Agent for the CustomJob's project is "
        "used.",
    )
    network: Optional[str] = Field(
        default=None,
        description="The full name of the Compute Engine network"
        "to which the Job should be peered. Private services access must "
        "already be configured for the network. If left unspecified, the job "
        "is not peered with any network.",
    )

    @sync_compatible
    async def run(
        self, task_status: Optional[TaskStatus] = None
    ) -> VertexAICustomTrainingJobResult:
        return VertexAICustomTrainingJobResult(
            identifier="not implemented", status_code=1
        )
