"""Module handling GCP credentials"""

import functools
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Union

from google.oauth2.service_account import Credentials
from prefect import get_run_logger

try:
    from google.cloud.bigquery import Client as BigQueryClient
except ModuleNotFoundError:
    pass  # will be raised in get_client

try:
    from google.cloud.secretmanager import SecretManagerServiceClient
except ModuleNotFoundError:
    pass

try:
    from google.cloud.storage import Client as StorageClient
except ModuleNotFoundError:
    pass


def _raise_help_msg(key: str):
    """
    Raises a helpful error message.
    Args:
        key: the key to access HELP_URLS
    """

    def outer(func):
        """
        Used for decorator.
        """

        @functools.wraps(func)
        def inner(*args, **kwargs):
            """
            Used for decorator.
            """
            logger = get_run_logger()
            try:
                return func(*args, **kwargs)
            except NameError:
                logger.exception(
                    f"Using `prefect_gcp.{key}` requires "
                    f"`pip install prefect_gcp[{key}]`"
                )
                raise

        return inner

    return outer


@dataclass
class GcpCredentials:
    """
    Dataclass used to manage authentication with GCP. GCP authentication is
    handled via the `google.oauth2` module or through the CLI.
    Specify either one of service account_file or service_account_info; if both
    are not specified, the client will try to detect the service account info stored
    in the env from the command, `gcloud auth application-default login`. Refer to the
    [Authentication docs](https://cloud.google.com/docs/authentication/production)
    for more info about the possible credential configurations.

    Args:
        service_account_file: Path to the service account JSON keyfile.
        service_account_info: The contents of the keyfile as a JSON string / dictionary.
        project: Name of the project to use.
    """

    service_account_file: Optional[Union[str, Path]] = None
    service_account_info: Optional[Union[str, Dict[str, str]]] = None
    project: Optional[str] = None

    @staticmethod
    def _get_credentials_from_service_account(
        service_account_file: Optional[str] = None,
        service_account_info: Optional[str] = None,
    ) -> Credentials:
        """
        Helper method to serialize credentials by using either
        service_account_file or service_account_info.
        """
        if service_account_info and service_account_file:
            raise ValueError(
                "Only one of service_account_info or service_account_file "
                "can be specified at once"
            )
        elif service_account_file:
            if not os.path.exists(service_account_file):
                raise ValueError("The provided path to the service account is invalid")
            elif isinstance(service_account_file, Path):
                service_account_file = service_account_file.expanduser()
            else:
                service_account_file = os.path.expanduser(service_account_file)
            credentials = Credentials.from_service_account_file(service_account_file)
        elif service_account_info:
            if isinstance(service_account_info, str):
                service_account_info = json.loads(service_account_info)
            credentials = Credentials.from_service_account_info(service_account_info)
        else:
            return None
        return credentials

    @_raise_help_msg("cloud_storage")
    def get_cloud_storage_client(self, project: Optional[str] = None) -> StorageClient:
        """
        Args:
            project: Name of the project to use; overrides the base
                class's project if provided.

        Examples:
            Gets a GCP Cloud Storage client from a path.
            ```python
            from prefect import flow
            from prefect_gcp.credentials import GcpCredentials

            @flow()
            def example_get_client_flow():
                service_account_file = "~/.secrets/prefect-service-account.json"
                client = GcpCredentials(
                    service_account_file=service_account_file
                ).get_cloud_storage_client()

            example_get_client_flow()
            ```

            Gets a GCP Cloud Storage client from a JSON dict.
            ```python
            from prefect import flow
            from prefect_gcp.credentials import GcpCredentials

            @flow()
            def example_get_client_flow():
                service_account_info = {
                    "type": "service_account",
                    "project_id": "project_id",
                    "private_key_id": "private_key_id",
                    "private_key": private_key",
                    "client_email": "client_email",
                    "client_id": "client_id",
                    "auth_uri": "auth_uri",
                    "token_uri": "token_uri",
                    "auth_provider_x509_cert_url": "auth_provider_x509_cert_url",
                    "client_x509_cert_url": "client_x509_cert_url"
                }
                client = GcpCredentials(
                    service_account_info=service_account_info
                ).get_cloud_storage_client()

            example_get_client_flow()
            ```
        """
        credentials = self._get_credentials_from_service_account(
            service_account_file=self.service_account_file,
            service_account_info=self.service_account_info,
        )

        # override class project if method project is provided
        project = project or self.project
        storage_client = StorageClient(credentials=credentials, project=project)
        return storage_client

    @_raise_help_msg("bigquery")
    def get_bigquery_client(
        self, project: str = None, location: str = None
    ) -> BigQueryClient:
        """
        Args:
            project: Name of the project to use; overrides the base
                class's project if provided.
            location: Location to use.

        Examples:
            Gets a GCP BigQuery client from a path.
            ```python
            from prefect import flow
            from prefect_gcp.credentials import GcpCredentials

            @flow()
            def example_get_client_flow():
                service_account_file = "~/.secrets/prefect-service-account.json"
                client = GcpCredentials(
                    service_account_file=service_account_file
                ).get_bigquery_client()

            example_get_client_flow()
            ```

            Gets a GCP BigQuery client from a dict.
            ```python
            from prefect import flow
            from prefect_gcp.credentials import GcpCredentials

            @flow()
            def example_get_client_flow():
                service_account_info = {
                    "type": "service_account",
                    "project_id": "project_id",
                    "private_key_id": "private_key_id",
                    "private_key": private_key",
                    "client_email": "client_email",
                    "client_id": "client_id",
                    "auth_uri": "auth_uri",
                    "token_uri": "token_uri",
                    "auth_provider_x509_cert_url": "auth_provider_x509_cert_url",
                    "client_x509_cert_url": "client_x509_cert_url"
                }
                client = GcpCredentials(
                    service_account_info=service_account_info
                ).get_bigquery_client(json)

            example_get_client_flow()
            ```
        """
        credentials = self._get_credentials_from_service_account(
            service_account_file=self.service_account_file,
            service_account_info=self.service_account_info,
        )

        # override class project if method project is provided
        project = project or self.project
        big_query_client = BigQueryClient(
            credentials=credentials, project=project, location=location
        )
        return big_query_client

    @_raise_help_msg("secret_manager")
    def get_secret_manager_client(self) -> SecretManagerServiceClient:
        """
        Args:
            project: Name of the project to use; overrides the base
                class's project if provided.

        Examples:
            Gets a GCP Secret Manager client from a path.
            ```python
            from prefect import flow
            from prefect_gcp.credentials import GcpCredentials

            @flow()
            def example_get_client_flow():
                service_account_file = "~/.secrets/prefect-service-account.json"
                client = GcpCredentials(
                    service_account_file=service_account_file
                ).get_secret_manager_client()

            example_get_client_flow()
            ```

            Gets a GCP Cloud Storage client from a JSON dict.
            ```python
            from prefect import flow
            from prefect_gcp.credentials import GcpCredentials

            @flow()
            def example_get_client_flow():
                service_account_info = {
                    "type": "service_account",
                    "project_id": "project_id",
                    "private_key_id": "private_key_id",
                    "private_key": private_key",
                    "client_email": "client_email",
                    "client_id": "client_id",
                    "auth_uri": "auth_uri",
                    "token_uri": "token_uri",
                    "auth_provider_x509_cert_url": "auth_provider_x509_cert_url",
                    "client_x509_cert_url": "client_x509_cert_url"
                }
                client = GcpCredentials(
                    service_account_info=service_account_info
                ).get_secret_manager_client()

            example_get_client_flow()
            ```
        """
        credentials = self._get_credentials_from_service_account(
            service_account_file=self.service_account_file,
            service_account_info=self.service_account_info,
        )

        # doesn't accept project; must pass in project in tasks
        secret_manager_client = SecretManagerServiceClient(credentials=credentials)
        return secret_manager_client
