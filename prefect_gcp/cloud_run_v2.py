import json
from typing import Literal, Optional

from anyio.abc._tasks import TaskStatus
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from googleapiclient.discovery import Resource
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import sync_compatible
from pydantic import BaseModel, Field

from prefect_gcp.credentials import GcpCredentials


class JobV2(BaseModel):
    """
    JobV2 is a data model for a job that will be run on Cloud Run with the V2 API.
    """

    name: str
    uid: str
    generation: str
    labels: dict[str, str]
    annotations: dict[str, str]
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
    binaryAuthorization: dict
    template: dict
    observedGeneration: str
    terminalConditions: dict
    conditions: list[dict]
    executionCount: int
    latestCreatedExecution: dict
    reconciling: bool
    satisfiesPzs: bool
    etag: str

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
            observedGeneration=response["observedGeneration"],
            terminalConditions=response.get("terminalConditions", {}),
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
        body: dict,
    ) -> dict:
        """
        Create a job on Cloud Run with the V2 API.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
            project (str): The GCP project ID.
            location (str): The GCP region.
            job_id (str): The ID of the job to create.
            body (dict): The job body.
        Returns:
            dict: The response from the Cloud Run V2 API.
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
    ) -> dict:
        """
        Delete a job on Cloud Run with the V2 API.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
            project (str): The GCP project ID.
            location (str): The GCP region.
            job_name (str): The name of the job to delete.
        Returns:
            dict: The response from the Cloud Run V2 API.
        """
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


class ExecutionV2(BaseModel):
    """
    ExecutionV2 is a data model for an execution of a job that will be run on
        Cloud Run API v2.
    """

    name: str
    uid: str
    generation: str
    labels: dict[str, str]
    annotations: dict[str, str]
    createTime: str
    startTime: Optional[str]
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
    template: dict
    reconciling: bool
    conditions: list[dict]
    observedGeneration: str
    runningCount: Optional[int]
    succeededCount: Optional[int]
    failedCount: Optional[int]
    cancelledCount: Optional[int]
    retriedCount: Optional[int]
    logUri: str
    satisfiesPzs: bool
    etag: str

    @classmethod
    def get(
        cls,
        cr_client: Resource,
        execution_name: str,
    ):
        """
        Get an execution from Cloud Run with the V2 API.

        Args:
            cr_client (Resource): The base client needed for interacting with GCP
                Cloud Run V2 API.
            execution_name (str): The name of the execution to get, in the form of
                projects/{project}/locations/{location}/jobs/{job}/executions
                    /{execution}
        """
        # noinspection PyUnresolvedReferences
        request = cr_client.jobs().executions().get(name=execution_name)

        response = request.execute()

        return cls(
            name=response["name"],
            uid=response["uid"],
            generation=response["generation"],
            labels=response.get("labels", {}),
            annotations=response.get("annotations", {}),
            createTime=response["createTime"],
            startTime=response.get("startTime"),
            deleteTime=response.get("deleteTime"),
            expireTime=response.get("expireTime"),
            launchStage=response.get("launchStage", "GA"),
            job=response["job"],
            parallelism=response["parallelism"],
            taskCount=response["taskCount"],
            template=response["template"],
            reconciling=response.get("reconciling", False),
            conditions=response.get("conditions", []),
            observedGeneration=response["observedGeneration"],
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

    args: Optional[list[str]] = Field(
        default_factory=list,
        description=(
            "The arguments to pass to the Cloud Run Job V2's entrypoint command."
        ),
    )
    binary_authorization: Optional[dict] = Field(
        default=None,
        description=(
            "The binary authorization policy for the Cloud Run Job V2. "
            "See https://cloud.google.com/run/docs/configuring/binary-authorization "
            "for additional details."
        ),
    )
    credentials: GcpCredentials  # cannot be a field; else it shows as JSON in UI
    cpu: Optional[int] = Field(
        default=None,
        title="CPU",
        description=(
            "The amount of CPU allocated to the Cloud Run Job V2. "
            "The int must be valid based on the rules specified at "
            "https://cloud.google.com/run/docs/configuring/cpu#setting-jobs ."
        ),
    )
    env: dict[str, str] = Field(
        default_factory=dict,
        description="The environment variables to pass to the Cloud Run Job V2.",
    )
    image: str = Field(
        ...,
        description=(
            "The image to use for a new Cloud Run Job V2. This value must "
            "refer to an image within either Google Container Registry "
            "or Google Artifact Registry, like `gcr.io/<project_name>/<repo>/`."
        ),
    )
    keep_job: Optional[bool] = Field(
        default=False,
        title="Keep Job after Completion",
        defscription=(
            "Whether to keep the Cloud Run Job V2 after it is completed. "
            "If False, the Cloud Run Job V2 will be deleted after completion."
        ),
    )
    labels: dict[str, str] = Field(
        default_factory=dict,
        description="The labels to pass to the Cloud Run Job V2.",
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
    memory: Optional[int] = Field(
        default=None,
        title="Memory",
        description="The amount of memory allocated to the Cloud Run Job V2.",
    )
    memory_unit: Optional[Literal["G", "Gi", "M", "Mi"]] = Field(
        default=None,
        title="Memory Unit",
        description=(
            "The unit of memory allocated to the Cloud Run Job V2. "
            "See https://cloud.google.com/run/docs/configuring/memory-limits#setting "
            "for additional details."
        ),
    )
    region: str = Field(
        ...,
        desciption="The region to run the Cloud Run Job V2 in.",
    )
    timeout: Optional[int] = Field(
        default=600,
        gt=0,
        le=86400,
        title="Job Timeout",
        description=(
            "The timeout for the Cloud Run Job V2 in seconds. Once the timeout "
            "is reached, an exception will be raised."
        ),
    )
    vpc_connector_name: Optional[str] = Field(
        default=None,
        title="VPC Connector Name",
        description="The name of the VPC connector to use for the Cloud Run Job V2.",
    )

    _job_name: str = None
    _execution_name: Optional[ExecutionV2] = None

    @sync_compatible
    async def run(
        self,
        task_status: TaskStatus | None = None,
    ) -> CloudRunJobV2Result:
        raise NotImplementedError

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
            str: A preview of the Cloud Run Job V2 .
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

    def _get_client(self) -> Resource:
        """
        Get the base client needed for interacting with GCP Cloud Run V2 API.
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
            )
            .projects()
            .locations()
        )

    def _memory_string(self) -> str | None:
        """
        Creates a properly formatted memory string for the Cloud Run V2 API POST
            CREATE request.

        Returns:
            str: The memory string or None if the memory AND memory unit are None.
        """
        if self.memory and self.memory_unit:
            return str(self.memory) + self.memory_unit

        return None

    def _job_body(self) -> dict:
        """
        Creates a properly formatted job body for the Cloud Run V2 API POST CREATE
        request.

        Returns:
            dict: The job body.
        """
        body = {
            "client": "prefect",
            "launchStage": self.launch_stage,
            "template": {
                "template": {
                    "containers": [
                        {
                            "env": [],
                            "image": self.image,
                            "command": self.command,
                            "args": self.args,
                            "resources": {
                                "limits": {
                                    "cpu": self.cpu,
                                    "memory": self._memory_string(),
                                },
                            },
                        },
                    ],
                }
            },
        }

        if self.labels:
            body["labels"].update(self.labels)

        if self.binary_authorization:
            body["binaryAuthorization"] = self.binary_authorization

        if self.env:
            body["template"]["template"]["containers"][0]["env"] = [self.env]

        return body


if __name__ == "__main__":
    job_run = CloudRunJobV2(
        credentials=GcpCredentials.load("gcp-credentials-latest"),
        region="us-central1",
        image="testaroo",
        memory=1024,
        memory_unit="Mi",
        cpu=1,
    )

    preview = job_run.preview(redact_values=False)

    print(preview)
