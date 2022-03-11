"""Module handling GCP credentials"""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Union

from google.cloud.storage import Client
from google.oauth2.service_account import Credentials


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
    project: str = None

    @staticmethod
    def _get_credentials_from_service_account(
        service_account_file=None,
        service_account_info=None,
    ) -> Credentials:
        """
        Helper method to serialize credentials by using either
        service_account_file or service_account_info.
        """
        file_is_none = service_account_file is None
        info_is_none = service_account_info is None
        if file_is_none and info_is_none:
            return None
        elif not file_is_none and not info_is_none:
            raise ValueError(
                "Only one of service_account_info or service_account_file "
                "can be specified at once"
            )
        elif service_account_file is not None:
            if not os.path.exists(service_account_file):
                raise ValueError("The provided path to the service account is invalid")
            else:
                service_account_file = os.path.expanduser(service_account_file)
            credentials = Credentials.from_service_account_file(service_account_file)
        else:
            if isinstance(service_account_info, str):
                service_account_info = json.loads(service_account_info)
            credentials = Credentials.from_service_account_info(service_account_info)
        return credentials

    def get_cloud_storage_client(self, project: str = None) -> Client:
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
                service_account_json_path = "~/.secrets/prefect-service-account.json"
                client = GcpCredentials(
                    service_account_json=service_account_json_path
                ).get_cloud_storage_client()

            example_get_client_flow()
            ```

            Gets a GCP Cloud Storage client from a dict.
            ```python
            from prefect import flow
            from prefect_gcp.credentials import GcpCredentials

            @flow()
            def example_get_client_flow():
                service_account_json = {
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
                    service_account_json=service_account_json
                ).get_cloud_storage_client(json)

            example_get_client_flow()
            ```
        """
        credentials = self._get_credentials_from_service_account(
            service_account_file=self.service_account_file,
            service_account_info=self.service_account_info,
        )

        # override class project if method project is provided
        project = project or self.project
        storage_client = Client(credentials=credentials, project=project)
        return storage_client
