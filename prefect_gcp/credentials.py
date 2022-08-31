"""Module handling GCP credentials"""

import functools
import json
import os
from pathlib import Path
from typing import Dict, Optional, Union

from google.oauth2.service_account import Credentials
from pydantic import Json, root_validator, validator
import google.auth.transport.requests

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

from prefect.blocks.core import Block


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
            try:
                return func(*args, **kwargs)
            except NameError as exc:
                raise ImportError(
                    f"To use prefect_gcp.{key}, install prefect-gcp with the "
                    f"'{key}' extra: `pip install 'prefect_gcp[{key}]'`"
                ) from exc

        return inner

    return outer


class GcpCredentials(Block):
    """
    Block used to manage authentication with GCP. GCP authentication is
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

    Example:
        Load stored GCP credentials:
        ```python
        from prefect_gcp import GcpCredentials
        gcp_credentials_block = GcpCredentials.load("BLOCK_NAME")
        ```
    """

    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/4CD4wwbiIKPkZDt4U3TEuW/c112fe85653da054b6d5334ef662bec4/gcp.png?h=250"  # noqa
    _block_type_name = "GCP Credentials"

    service_account_file: Optional[Path] = None
    service_account_info: Optional[Json] = None

    @root_validator
    def provide_one_service_account_source(cls, values):
        if values.get("service_account_info") is not None and values.get("service_account_file") is not None:
            raise ValueError(
                "Only one of service_account_info or service_account_file "
                "can be specified at once"
            )
        if values.get("service_account_info") is None and values.get("service_account_file") is None:
            raise ValueError(
                "You must provide either service_account_info or service_account_file "
                "to create a GcpCredentails block."
            )
        return values

    @validator('service_account_file')
    def check_service_account_file_exists(cls, file):
        if not os.path.exists(file):
            raise ValueError("The provided path to the service account is invalid")
        elif isinstance(file, Path):
            service_account_file = file.expanduser()
        else:
            service_account_file = os.path.expanduser(file)
        return file

    def get_project_id(self):
        return self.get_credentials_from_service_account().project_id

    def get_credentials_from_service_account(self) -> Union[Credentials, None]:
        """
        Helper method to serialize credentials by using either
        service_account_file or service_account_info.
        """
        if self.service_account_file:
            credentials = Credentials.from_service_account_file(self.service_account_file, scopes=['https://www.googleapis.com/auth/cloud-platform'])
        elif self.service_account_info:
            credentials = Credentials.from_service_account_info(self.service_account_info, scopes=['https://www.googleapis.com/auth/cloud-platform'])
        else:
            return None
        return credentials

    def get_service_account_value(self) -> Json:
        if self.service_account_file:
            with open(self.service_account_file, "r") as f:
                value = json.load(f)
        else:
            value = self.service_account_info
        
        return value
        
    def get_access_token(self):
        """
        See: https://stackoverflow.com/a/69107745
        also: https://www.jhanley.com/google-cloud-creating-oauth-access-tokens-for-rest-api-calls/
        """
        request = google.auth.transport.requests.Request()
        credentials = self.get_credentials_from_service_account()
        credentials.refresh(request)

        return credentials.token

    @_raise_help_msg("cloud_storage")
    def get_cloud_storage_client(
        self, project: Optional[str] = None
    ) -> "StorageClient":
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

            Gets a GCP Cloud Storage client from a JSON str.
            ```python
            import json
            from prefect import flow
            from prefect_gcp.credentials import GcpCredentials

            @flow()
            def example_get_client_flow():
                service_account_info = json.dumps({
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
                })
                client = GcpCredentials(
                    service_account_info=service_account_info
                ).get_cloud_storage_client()

            example_get_client_flow()
            ```
        """
        credentials = self.get_credentials_from_service_account(
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
    ) -> "BigQueryClient":
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

            Gets a GCP BigQuery client from a JSON str.
            ```python
            import json
            from prefect import flow
            from prefect_gcp.credentials import GcpCredentials

            @flow()
            def example_get_client_flow():
                service_account_info = json.dumps({
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
                })
                client = GcpCredentials(
                    service_account_info=service_account_info
                ).get_bigquery_client(json)

            example_get_client_flow()
            ```
        """
        credentials = self.get_credentials_from_service_account(
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
    def get_secret_manager_client(self) -> "SecretManagerServiceClient":
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

            Gets a GCP Cloud Storage client from a JSON str.
            ```python
            import json
            from prefect import flow
            from prefect_gcp.credentials import GcpCredentials

            @flow()
            def example_get_client_flow():
                service_account_info = json.dumps({
                    "type": "service_account",
                    "project_id": "project_id",
                    "private_key_id": "private_key_id",
                    "private_key": "private_key",
                    "client_email": "client_email",
                    "client_id": "client_id",
                    "auth_uri": "auth_uri",
                    "token_uri": "token_uri",
                    "auth_provider_x509_cert_url": "auth_provider_x509_cert_url",
                    "client_x509_cert_url": "client_x509_cert_url"
                })
                client = GcpCredentials(
                    service_account_info=service_account_info
                ).get_secret_manager_client()

            example_get_client_flow()
            ```
        """
        credentials = self.get_credentials_from_service_account(
            service_account_file=self.service_account_file,
            service_account_info=self.service_account_info,
        )

        # doesn't accept project; must pass in project in tasks
        secret_manager_client = SecretManagerServiceClient(credentials=credentials)
        return secret_manager_client
