from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
from prefect.infrastructure import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import sync_compatible
from pydantic import BaseModel


class BaseJob(BaseModel, ABC):
    """ToDo"""

    @abstractmethod
    def is_ready(self):
        """ToDo"""

    @abstractmethod
    def _is_missing_container(self):
        """ToDo"""

    @abstractmethod
    def has_execution_in_progress(self):
        """ToDo"""

    @abstractmethod
    def _get_ready_condition(self):
        """ToDo"""

    @staticmethod
    @abstractmethod
    def _get_execution_status():
        """ToDo"""

    @classmethod
    @abstractmethod
    def get(cls, *args, **kwargs):
        """ToDo"""

    @staticmethod
    @abstractmethod
    def create(*arg, **kwargs):
        """ToDo"""

    @staticmethod
    @abstractmethod
    def delete(*arg, **kwargs):
        """ToDo"""

    @staticmethod
    @abstractmethod
    def run(*arg, **kwargs):
        """ToDo"""


class BaseExecution(BaseModel, ABC):
    """ToDo"""

    @abstractmethod
    def is_running(self):
        """ToDo"""

    @abstractmethod
    def condition_after_completion(self):
        """ToDo"""

    @abstractmethod
    def succeeded(self):
        """ToDo"""

    @classmethod
    @abstractmethod
    def get(cls, *args, **kwargs):
        """ToDo"""


class BaseCloudRunJobResult(InfrastructureResult, ABC):
    """ToDo"""


class BaseCloudRunJob(Infrastructure):
    """ToDo"""

    _blog_type_slug = NotImplemented
    _block_type_name = NotImplemented
    _description = NotImplemented
    _logo_url = NotImplemented
    _documentation_url = NotImplemented

    @property
    @abstractmethod
    def job_name(self):
        """ToDo"""

    @property
    @abstractmethod
    def memory_string(self):
        """ToDo"""

    @abstractmethod
    def _remove_image_spaces(self, value: Optional[str]):
        """ToDo"""

    @abstractmethod
    def _check_valid_memory(self, values: Dict):
        """ToDo"""

    def _create_job_error(self, exc: HttpError):
        """ToDo"""

    def _cpu_as_k8s_quantity(self):
        """ToDo"""

    @sync_compatible
    @abstractmethod
    async def kill(self, identifier: str, grace_seconds: int):
        """ToDo"""

    @abstractmethod
    def _kill_job(self, *args, **kwargs):
        """ToDo"""

    @abstractmethod
    def _create_job_and_wait_for_registration(self, client: Resource):
        """ToDo"""

    @abstractmethod
    def _begin_job_execution(self, client: Resource):
        """ToDo"""

    @abstractmethod
    def _watch_job_execution_and_get_result(self, *args, **kwargs):
        """ToDo"""

    @abstractmethod
    def _jobs_body(self):
        """ToDo"""

    @abstractmethod
    def preview(self):
        """ToDo"""

    @abstractmethod
    def _watch_job_execution(self, *args, **kwargs):
        """ToDo"""

    @abstractmethod
    def _wait_for_job_creation(
        self,
        client: Resource,
        timeout: int,
        poll_interval: int,
    ):
        """ToDo"""

    @abstractmethod
    def _get_client(self):
        """ToDo"""

    @abstractmethod
    def _add_container_settings(self, base_settings: Dict[str, Any]):
        """ToDo"""

    @abstractmethod
    def _add_args(self):
        """ToDo"""

    @abstractmethod
    def _add_command(self):
        """ToDo"""

    @abstractmethod
    def _add_resources(self):
        """ToDo"""

    @abstractmethod
    def _add_env(self):
        """ToDo"""
