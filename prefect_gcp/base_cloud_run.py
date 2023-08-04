import re
from abc import ABC, abstractmethod
from typing import Any, Dict, Literal, Optional
from uuid import uuid4

from anyio.abc import TaskStatus
from googleapiclient.discovery import Resource
from prefect.infrastructure import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import BaseModel, Field, root_validator, validator


class BaseJob(BaseModel, ABC):
    """
    Base class for the utility classes to call GCP `jobs` API and
    interact with the returned objects.
    """

    @abstractmethod
    def is_ready(self):
        """
        Reports whether a job is finished registering and ready to be executed.
        """

    @abstractmethod
    def _is_missing_container(self):
        """
        Check if Job status is not ready because the specified container cannot
        be found.
        """

    @abstractmethod
    def has_execution_in_progress(self):
        """
        See if job has an execution in progress.
        """

    @classmethod
    @abstractmethod
    def get(cls, *args, **kwargs):
        """
        Makes a GET request to the GCP jobs API to fetch a instance of a specified Job.
        """

    @staticmethod
    @abstractmethod
    def create(*arg, **kwargs):
        """
        Make a create request to the GCP jobs API.
        """

    @staticmethod
    @abstractmethod
    def delete(*arg, **kwargs):
        """
        Make a delete request to the GCP jobs API.
        """

    @staticmethod
    @abstractmethod
    def run(*arg, **kwargs):
        """
        Makes a run request to the GCP jobs API.
        """


class BaseExecution(BaseModel, ABC):
    """
    Base class for utility classes to call GCP `executions` API and
    interact with the returned objects.
    """

    @abstractmethod
    def is_running(self):
        """
        Indicates whether an Execution is still running or not.
        """

    @abstractmethod
    def condition_after_completion(self):
        """
        Looks for the Execution condition that indicates if an execution is finished.
        """

    @abstractmethod
    def succeeded(self):
        """
        Checks whether or not the Execution completed is a successful state.
        """

    @classmethod
    @abstractmethod
    def get(cls, *args, **kwargs):
        """
        Makes a get request to the GCP executions API and return an Execution instance.
        """


class BaseCloudRunJobResult(InfrastructureResult, ABC):
    """
    Result from a Cloud Run Job.
    """


class BaseCloudRunJob(Infrastructure, ABC):
    """
    Base class for Cloud Run Job classes.
    The classes that inherit this are used to run GCP Cloud Run Jobs.
    """

    _block_type_slug: str = "base-cloud-run-job"
    _block_type_name: str = "Base GCP Cloud Run Job"
    _description: str = "Base Infrastructure block used to run GCP Cloud Run Jobs. Note this block is experimental. The interface may change without notice."  # noqa
    _logo_url: str = "https://images.ctfassets.net/gm98wzqotmnx/4CD4wwbiIKPkZDt4U3TEuW/c112fe85653da054b6d5334ef662bec4/gcp.png?h=250"  # noqa
    _documentation_url: str = "https://prefecthq.github.io/prefect-gcp/cloud_run/#prefect_gcp.cloud_run.BaseCloudRunJob"  # noqa: E501

    _job_name: str = ""

    type: Literal["base-cloud-run-job"] = Field(
        "base-cloud-run-job",
        description="The slug for this task type.",
    )

    @property
    def job_name(self) -> str:
        """
        Create a unique and valid job name.

        Returns:
            A unique a valid job name string.
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
    def memory_string(self):
        """
        Generates a Memory string and returns it.

        Returns:
            A property formatted memory string for Cloud Run API Run Job.
        """
        if self.memory and self.memory_unit:
            return str(self.memory) + self.memory_unit
        return None

    @validator("image", check_fields=False)
    def _remove_image_spaces(cls, value: Optional[str]) -> str or None:
        """
        Handles spaces in image names.

        Args:
            value: A value that is either a string or None.
        Returns:
            If the Image Name is None, it returns None, otherwise it returns the Image
            Name after having the spaces stripped.
        """
        if value is not None:
            return value.strip()

    @root_validator
    def _check_valid_memory(cls, values: Dict) -> Dict:
        """
        Makes sure memory conforms to expected values for API.
        See: https://cloud.google.com/run/docs/configuring/memory-limits#setting

        Args:
            values: A dictionary containing the memory information.

        Returns:
            A dictionary with
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
        # ToDo: consider lookup table instead of the if/else also
        #  check for documented errors
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
        Generates the CPU Integer in the format expected by GCP Cloud Run Jobs APIs.
        See:
         https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
        See also: https://cloud.google.com/run/docs/configuring/cpu#setting-jobs

        Returns:
             The CPU integer in the format expected by GCP Cloud Run Jobs APIs.
        """
        return str(self.cpu * 1000) + "m"

    @sync_compatible
    async def run(self, task_status: Optional[TaskStatus] = None) -> Any:
        """
        Runs the configured job on a Google Cloud Run Job.

        Args:
            task_status: The optional Task Status.

        Returns:
            The result of the run.
        """
        with self._get_client() as client:
            await run_sync_in_worker_thread(
                self._create_job_and_wait_for_registration, client
            )
            job_execution = await run_sync_in_worker_thread(
                self._begin_job_execution,
                client,
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
    @abstractmethod
    async def kill(self, identifier: str, grace_seconds: int = 30):
        """
        Kill a task running Cloud Run.
        """

    @abstractmethod
    def _kill_job(self, *args, **kwargs):
        """
        Thin wrapper around Job.delete, wrapping a try/except since
        Job is an independent class that doesn't have knowledge of
        CloudRunJob and its associated logic.
        """

    @abstractmethod
    def _create_job_and_wait_for_registration(self, client: Resource):
        """
        Creates a new job wait for it to finish registering.

        Args:
            client: The GCP Cloud Run API client.
        """

    @abstractmethod
    def _begin_job_execution(self, client: Resource):
        """
        Submits a job run for execution and return the execution object.

        Args:
            client: The GCP Cloud Run API client.
        """

    @abstractmethod
    def _watch_job_execution_and_get_result(self, *args, **kwargs):
        """
        Waits for execution to complete and then return result.
        """

    @abstractmethod
    def _jobs_body(self):
        """
        Creates properly formatted body used for a Job create request.
        """

    @abstractmethod
    def preview(self):
        """
        Generates a preview of the job definition that will be sent to GCP.
        """

    @abstractmethod
    def _wait_for_job_creation(
        self,
        client: Resource,
        timeout: int,
        poll_interval: int,
    ):
        """
        Gives created job time to register.

        Args:
            client: The GCP Cloud Run API client.
            timeout: The maximum runtime for the Job until it times out.
            poll_interval: The number of second between each status poll request.
        """

    @abstractmethod
    def _get_client(self):
        """
        Get the base client needed for interacting with GCP APIs.
        """

    def _add_container_settings(
        self,
        base_settings: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Adds settings related to containers for Cloud Run Jobs to a dictionary.
        Includes environment variables, entrypoint command, entrypoint arguments,
        and cpu and memory limits.

        Args:
            base_settings: The Dictionary of base settings for the container settings.

        Returns:
            A dictionary with all of the container settings.
        """
        container_settings = base_settings.copy()
        container_settings.update(self._add_env())
        container_settings.update(self._add_resources())
        container_settings.update(self._add_command())
        container_settings.update(self._add_args())

        return container_settings

    def _add_args(self) -> Dict:
        """
        Sets the arguments that will be passed to the entrypoint for a Cloud Run Job.

        Returns:
            A dictionary with the added arguments added.
        """
        return {"args": self.args} if self.args else {}

    def _add_command(self) -> Dict:
        """
        Sets the command that a container will run for a Cloud Run Job.

        Returns:
            A dictionary with the added commands added.
        """
        return {"command": self.command}

    @abstractmethod
    def _add_resources(self):
        """
        Sets specified resources limits for a Cloud Run Job.
        """

    def _add_env(self) -> Dict:
        """
        Adds environment variables for a Cloud Run Job.

        Method `self._base_environment()` gets necessary Prefect environment variables
        from the config.

        Returns:
            A dictionary with the Cloud Run environment variables.
        """
        env = {**self._base_environment(), **self.env}
        cloud_run_env = [{"name": k, "value": v} for k, v in env.items()]
        return {"env": cloud_run_env}

    @staticmethod
    @abstractmethod
    def _watch_job_execution(*args, **kwargs):
        """
        Updates job_execution status until it is no longer running or timeout is
        reached.
        """
