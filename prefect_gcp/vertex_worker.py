""" <!-- # noqa -->

Module containing the custom worker used for executing flow runs as Vertex AI Custom Jobs.

Get started by creating a Cloud Run work pool:

```bash
prefect work-pool create 'my-vertex-pool' --type vertex-ai
```

Then start a Cloud Run worker with the following command:

```bash
prefect worker start --pool 'my-vertex-pool'
```

## Configuration
Read more about configuring work pools
[here](https://docs.prefect.io/latest/concepts/work-pools/#work-pool-overview).
"""
import datetime
import re
import shlex
import time
from typing import TYPE_CHECKING, Dict, List, MutableSequence, Optional, Tuple
from uuid import uuid4

import anyio
from prefect.logging.loggers import PrefectLogAdapter
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import Field
from slugify import slugify

from prefect_gcp.credentials import GcpCredentials

# to prevent "Failed to load collection" from surfacing
# if google-cloud-aiplatform is not installed
try:
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
    from google.cloud.aiplatform_v1.types.machine_resources import DiskSpec, MachineSpec
    from google.protobuf.duration_pb2 import Duration
    from tenacity import retry, stop_after_attempt, wait_fixed, wait_random
except ModuleNotFoundError:
    pass

_DISALLOWED_GCP_LABEL_CHARACTERS = re.compile(r"[^-a-zA-Z0-9_]+")

if TYPE_CHECKING:
    from prefect.client.schemas import FlowRun


class VertexAIWorkerVariables(BaseVariables):
    """
    Default variables for the Vertex AI worker.

    The schema for this class is used to populate the `variables` section of the default
    base job template.
    """

    region: str = Field(
        description="The region where the Vertex AI Job resides.",
        example="us-central1",
    )
    image: str = Field(
        title="Image Name",
        description=(
            "The URI of a container image in the Container or Artifact Registry, "
            "used to run your Vertex AI Job. Note that Vertex AI will need access"
            "to the project and region where the container image is stored. See "
            "https://cloud.google.com/vertex-ai/docs/training/create-custom-container"
        ),
        example="gcr.io/your-project/your-repo:latest",
    )
    credentials: Optional[GcpCredentials] = Field(
        title="GCP Credentials",
        default_factory=GcpCredentials,
        description="The GCP Credentials used to initiate the "
        "Vertex AI Job. If not provided credentials will be "
        "inferred from the local environment.",
    )
    machine_type: str = Field(
        title="Machine Type",
        description=(
            "The machine type to use for the run, which controls "
            "the available CPU and memory. "
            "See https://cloud.google.com/vertex-ai/docs/reference/rest/v1/MachineSpec"
        ),
        example="n1-standard-4",
    )
    accelerator_type: Optional[str] = Field(
        title="Accelerator Type",
        description=(
            "The type of accelerator to attach to the machine. "
            "See https://cloud.google.com/vertex-ai/docs/reference/rest/v1/MachineSpec"
        ),
        example="NVIDIA_TESLA_K80",
    )
    accelerator_count: Optional[int] = Field(
        title="Accelerator Count",
        description=(
            "The number of accelerators to attach to the machine. "
            "See https://cloud.google.com/vertex-ai/docs/reference/rest/v1/MachineSpec"
        ),
    )
    boot_disk_type: str = Field(
        title="Boot Disk Type",
        description="The type of boot disk to attach to the machine.",
        example="pd-ssd",
    )
    boot_disk_size_gb: int = Field(
        title="Boot Disk Size (GB)",
        description="The size of the boot disk to attach to the machine, in gigabytes.",
        example=25,
    )
    maximum_run_time_hours: int = Field(
        default=1,
        title="Maximum Run Time (Hours)",
        description="The maximum job running time, in hours",
    )
    network: Optional[str] = Field(
        default=None,
        title="Network",
        description="The full name of the Compute Engine network"
        "to which the Job should be peered. Private services access must "
        "already be configured for the network. If left unspecified, the job "
        "is not peered with any network. "
        "For example: projects/12345/global/networks/myVPC",
    )
    reserved_ip_ranges: Optional[List[str]] = Field(
        default=None,
        title="Reserved IP Ranges",
        description="A list of names for the reserved ip ranges under the VPC "
        "network that can be used for this job. If set, we will deploy the job "
        "within the provided ip ranges. Otherwise, the job will be deployed to "
        "any ip ranges under the provided VPC network.",
    )
    service_account_name: Optional[str] = Field(
        default=None,
        title="Service Account Name",
        description=(
            "Specifies the service account to use "
            "as the run-as account in Vertex AI. The worker submitting jobs must have "
            "act-as permission on this run-as account. If unspecified, the AI "
            "Platform Custom Code Service Agent for the CustomJob's project is "
            "used. Takes precedence over the service account found in GCP credentials, "
            "and required if a service account cannot be detected in GCP credentials."
        ),
    )
    job_watch_poll_interval: float = Field(
        default=5.0,
        title="Poll Interval (Seconds)",
        description=(
            "The amount of time to wait between GCP API calls while monitoring the "
            "state of a Vertex AI Job."
        ),
    )


class VertexAIWorkerJobConfiguration(BaseJobConfiguration):
    """
    Configuration class used by the Vertex AI Worker to create a Job.

    An instance of this class is passed to the Vertex AI Worker's `run` method
    for each flow run. It contains all information necessary to execute
    the flow run as a Vertex AI Job.

    Attributes:
        region: The region where the Vertex AI Job resides.
        image: The URI of a container image in the Container or Artifact Registry.
        credentials: The GCP Credentials used to connect to Vertex AI.
        machine_type: The machine type to use for the run, which controls resources.
        accelerator_type: The type of accelerator to attach to the machine.
        accelerator_count: The number of accelerators to attach to the machine.
        boot_disk_type: The type of boot disk to attach to the machine.
        boot_disk_size_gb: The size of the boot disk to attach to the machine.
        maximum_run_time_hours: The maximum job running time, in hours.
        network: The full name of the GCE network to which the Job should be peered.
        reserved_ip_ranges: A list of reserved ip names, where this Job would run.
        service_account_name: The service account to use as in the Vertex AI Job.
        job_watch_poll_interval: The interval between GCP API calls to check Job state.
    """

    region: str = Field(
        description="The region where the Vertex AI Job resides.",
        example="us-central1",
    )
    image: str = Field(
        title="Image Name",
        description=(
            "The URI of a container image in the Container or Artifact Registry, "
            "used to run your Vertex AI Job. Note that Vertex AI will need access"
            "to the project and region where the container image is stored. See "
            "https://cloud.google.com/vertex-ai/docs/training/create-custom-container"
        ),
        example="gcr.io/your-project/your-repo:latest",
    )
    credentials: Optional[GcpCredentials] = Field(
        title="GCP Credentials",
        default_factory=GcpCredentials,
        description="The GCP Credentials used to initiate the "
        "Vertex AI Job. If not provided credentials will be "
        "inferred from the local environment.",
    )
    machine_type: str = Field(
        default="n1-standard-4",
        title="Machine Type",
        description=(
            "The machine type to use for the run, which controls "
            "the available CPU and memory. "
            "See https://cloud.google.com/vertex-ai/docs/reference/rest/v1/MachineSpec"
        ),
    )
    accelerator_type: Optional[str] = Field(
        title="Accelerator Type",
        description=(
            "The type of accelerator to attach to the machine. "
            "See https://cloud.google.com/vertex-ai/docs/reference/rest/v1/MachineSpec"
        ),
    )
    accelerator_count: Optional[int] = Field(
        title="Accelerator Count",
        description=(
            "The number of accelerators to attach to the machine. "
            "See https://cloud.google.com/vertex-ai/docs/reference/rest/v1/MachineSpec"
        ),
    )
    boot_disk_type: str = Field(
        default="pd-ssd",
        title="Boot Disk Type",
        description="The type of boot disk to attach to the machine.",
    )
    boot_disk_size_gb: int = Field(
        default=25,
        title="Boot Disk Size (GB)",
        description="The size of the boot disk to attach to the machine, in gigabytes.",
        example=25,
    )
    maximum_run_time_hours: int = Field(
        default=1,
        title="Maximum Run Time (Hours)",
        description="The maximum job running time, in hours",
        example=1,
    )
    network: Optional[str] = Field(
        default=None,
        title="Network",
        description="The full name of the Compute Engine network"
        "to which the Job should be peered. Private services access must "
        "already be configured for the network. If left unspecified, the job "
        "is not peered with any network. "
        "For example: projects/12345/global/networks/myVPC",
    )
    reserved_ip_ranges: Optional[List[str]] = Field(
        default=None,
        title="Reserved IP Ranges",
        description="A list of names for the reserved ip ranges under the VPC "
        "network that can be used for this job. If set, we will deploy the job "
        "within the provided ip ranges. Otherwise, the job will be deployed to "
        "any ip ranges under the provided VPC network.",
    )
    service_account_name: Optional[str] = Field(
        default=None,
        title="Service Account Name",
        description=(
            "Specifies the service account to use "
            "as the run-as account in Vertex AI. The worker submitting jobs must have "
            "act-as permission on this run-as account. If unspecified, the AI "
            "Platform Custom Code Service Agent for the CustomJob's project is "
            "used. Takes precedence over the service account found in GCP credentials, "
            "and required if a service account cannot be detected in GCP credentials."
        ),
    )
    job_watch_poll_interval: float = Field(
        default=5.0,
        title="Poll Interval (Seconds)",
        description=(
            "The amount of time to wait between GCP API calls while monitoring the "
            "state of a Vertex AI Job."
        ),
    )

    @property
    def project(self) -> str:
        """property for accessing the project from the credentials."""
        return self.credentials.project

    @property
    def job_name(self) -> str:
        """
        The name can be up to 128 characters long and can be consist of any UTF-8 characters. Reference:
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
        job_name = f"{repo_name}-{unique_suffix}"
        return job_name

    def command_as_list(self) -> MutableSequence[str]:
        """worker command strings are converted to a list of strings"""
        if not self.command:
            return ["python", "-m", "prefect.engine"]

        return shlex.split(self.command)


class VertexAIWorkerResult(BaseWorkerResult):
    """Contains information about the final state of a completed process"""


class VertexAIWorker(BaseWorker):
    """Prefect worker that executes flow runs within Vertex AI Jobs."""

    type = "vertex-ai"
    job_configuration = VertexAIWorkerJobConfiguration
    job_configuration_variables = VertexAIWorkerVariables
    _description = (
        "Execute flow runs within containers on Google Vertex AI. Requires "
        "a Google Cloud Platform account."
    )
    _display_name = "Google Vertex AI"
    _documentation_url = "https://prefecthq.github.io/prefect-gcp/worker/"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/4SpnOBvMYkHp6z939MDKP6/549a91bc1ce9afd4fb12c68db7b68106/social-icon-google-cloud-1200-630.png?h=250"  # noqa

    async def run(
        self,
        flow_run: "FlowRun",
        configuration: VertexAIWorkerJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> VertexAIWorkerResult:
        """
        Executes a flow run within a Vertex AI Job and waits for the flow run
        to complete.

        Args:
            flow_run: The flow run to execute
            configuration: The configuration to use when executing the flow run.
            task_status: The task status object for the current flow run. If provided,
                the task will be marked as started.

        Returns:
            VertexAIWorkerResult: A result object containing information about the
                final state of the flow run
        """
        logger = self.get_flow_run_logger(flow_run)

        client_options = ClientOptions(
            api_endpoint=f"{configuration.region}-aiplatform.googleapis.com"
        )

        job_spec = self._build_job_spec(configuration)
        with configuration.credentials.get_job_service_client(
            client_options=client_options
        ) as job_service_client:
            job_run = await self._create_and_begin_job(
                job_spec, job_service_client, configuration, logger
            )

            if task_status:
                task_status.started(configuration.job_name)

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
                configuration=configuration,
                logger=logger,
                timeout=int(
                    datetime.timedelta(
                        hours=configuration.maximum_run_time_hours
                    ).total_seconds()
                ),
            )

        error_msg = final_job_run.error.message
        if error_msg:
            raise RuntimeError(error_msg)

        status_code = 0 if final_job_run.state == JobState.JOB_STATE_SUCCEEDED else 1

        return VertexAIWorkerResult(
            identifier=final_job_run.display_name, status_code=status_code
        )

    def _build_job_spec(
        self, configuration: VertexAIWorkerJobConfiguration
    ) -> "CustomJobSpec":
        """
        Builds a job spec by gathering details.
        """
        # gather worker pool spec
        env_list = [
            {"name": name, "value": value}
            for name, value in {
                **configuration.env,
            }.items()
        ]

        container_spec = ContainerSpec(
            image_uri=configuration.image,
            command=configuration.command_as_list(),
            args=[],
            env=env_list,
        )
        machine_spec = MachineSpec(
            machine_type=configuration.machine_type,
            accelerator_type=configuration.accelerator_type,
            accelerator_count=configuration.accelerator_count,
        )
        worker_pool_spec = WorkerPoolSpec(
            container_spec=container_spec,
            machine_spec=machine_spec,
            replica_count=1,
            disk_spec=DiskSpec(
                boot_disk_type=configuration.boot_disk_type,
                boot_disk_size_gb=configuration.boot_disk_size_gb,
            ),
        )
        # look for service account
        service_account = (
            configuration.service_account_name
            or configuration.credentials._service_account_email
        )
        if service_account is None:
            raise ValueError(
                "A service account is required for the Vertex job. "
                "A service account could not be detected in the attached credentials "
                "or in the service_account_name input. "
                "Please pass in valid GCP credentials or a valid service_account_name"
            )

        # build custom job specs
        timeout = Duration().FromTimedelta(
            td=datetime.timedelta(hours=configuration.maximum_run_time_hours)
        )
        scheduling = Scheduling(timeout=timeout)
        job_spec = CustomJobSpec(
            worker_pool_specs=[worker_pool_spec],
            service_account=service_account,
            scheduling=scheduling,
            network=configuration.network,
            reserved_ip_ranges=configuration.reserved_ip_ranges,
        )
        return job_spec

    async def _create_and_begin_job(
        self,
        job_spec: "CustomJobSpec",
        job_service_client: "JobServiceClient",
        configuration: VertexAIWorkerJobConfiguration,
        logger: PrefectLogAdapter,
    ) -> "CustomJob":
        """
        Builds a custom job and begins running it.
        """
        # create custom job
        custom_job = CustomJob(
            display_name=configuration.job_name,
            job_spec=job_spec,
            labels=self._get_compatible_labels(configuration=configuration),
        )

        # run job
        logger.info(
            f"Job {configuration.job_name!r} starting to run "
            f"the command {configuration.command!r} in region "
            f"{configuration.region!r} using image {configuration.image!r}"
        )

        project = configuration.project
        resource_name = f"projects/{project}/locations/{configuration.region}"

        retry_policy = retry(
            stop=stop_after_attempt(3), wait=wait_fixed(1) + wait_random(0, 3)
        )

        custom_job_run = await run_sync_in_worker_thread(
            retry_policy(job_service_client.create_custom_job),
            parent=resource_name,
            custom_job=custom_job,
        )

        logger.info(
            f"Job {configuration.job_name!r} has successfully started; "
            f"the full job name is {custom_job_run.name!r}"
        )

        return custom_job_run

    async def _watch_job_run(
        self,
        full_job_name: str,  # different from self.job_name
        job_service_client: "JobServiceClient",
        current_state: "JobState",
        until_states: Tuple["JobState"],
        configuration: VertexAIWorkerJobConfiguration,
        logger: PrefectLogAdapter,
        timeout: int = None,
    ) -> "CustomJob":
        """
        Polls job run to see if status changed.
        """
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
                logger.info(f"{configuration.job_name} has new {state_label}")
                last_state = state
            else:
                # Intermittently, the job will not be described. We want to respect the
                # watch timeout though.
                logger.debug(f"Job {configuration.job_name} not found.")

            elapsed_time = time.time() - t0
            if timeout is not None and elapsed_time > timeout:
                raise RuntimeError(
                    f"Timed out after {elapsed_time}s while watching job for states "
                    "{until_states!r}"
                )
            time.sleep(configuration.job_watch_poll_interval)

        return job_run

    def _get_compatible_labels(
        self, configuration: VertexAIWorkerJobConfiguration
    ) -> Dict[str, str]:
        """
        Ensures labels are compatible with GCP label requirements.
        https://cloud.google.com/resource-manager/docs/creating-managing-labels

        Ex: the Prefect provided key of prefect.io/flow-name -> prefect-io_flow-name
        """
        compatible_labels = {}
        for key, val in configuration.labels.items():
            new_key = slugify(
                key,
                lowercase=True,
                replacements=[("/", "_"), (".", "-")],
                max_length=63,
                regex_pattern=_DISALLOWED_GCP_LABEL_CHARACTERS,
            )
            compatible_labels[new_key] = slugify(
                val,
                lowercase=True,
                replacements=[("/", "_"), (".", "-")],
                max_length=63,
                regex_pattern=_DISALLOWED_GCP_LABEL_CHARACTERS,
            )
        return compatible_labels
