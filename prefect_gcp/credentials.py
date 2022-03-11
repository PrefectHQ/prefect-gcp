"""Module handling GCP credentials"""

import os
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Union, Dict, Optional

from google.oauth2.service_account import Credentials
from google.cloud.storage import Client


@dataclass
class GCPCredentials:
    """
    Dataclass used to manage authentication with GCP. GCP authentication is
    handled via the `google.oauth2` module or through the CLI. Refer to the
    [Authentication docs](https://cloud.google.com/docs/authentication/production)
    for more info about the possible credential configurations.
    Args:
        service_account_json: Path to the service account JSON keyfile or
            the JSON string / dictionary. If not provided 
        project: Name of the project to use.
    """

    service_account_json: Optional[Union[Dict[str, str], str, Path]] = None
    project: str = None

    @staticmethod
    def _get_credentials_from_service_account(service_account_json) -> Credentials:
        if service_account_json is None:
            return None

        if isinstance(service_account_json, Path):
            service_account_json = str(service_account_json)

        is_str = isinstance(service_account_json, str)
        if isinstance(service_account_json, dict):
            # if is a JSON dict
            credentials = Credentials.from_service_account_info(service_account_json)
        elif is_str and "{" in service_account_json:
            # if is a JSON string
            service_account_json = json.loads(service_account_json)
            credentials = Credentials.from_service_account_info(service_account_json)
        elif is_str:
            # if is a path string
            if "~" in service_account_json:
                service_account_json = os.path.expanduser(service_account_json)
            elif "$HOME" in service_account_json:
                service_account_json = os.path.expandvars(service_account_json)
            if not os.path.exists(service_account_json):
                raise ValueError(f"The provided path to the service account is invalid")
            credentials = Credentials.from_service_account_file(service_account_json)
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
            from prefect_gcp.credentials import GCPCredentials

            @flow()
            def example_get_client_flow():
                service_account_json_path = "~/.secrets/prefect-service-account.json"
                client = GCPCredentials(
                    service_account_json=service_account_json_path
                ).get_client()

            test_flow()
            ```

            Gets a GCP Cloud Storage client from a dict.
            ```python
            from prefect import flow
            from prefect_gcp.credentials import GCPCredentials

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
                client = GCPCredentials(
                    service_account_json=service_account_json
                ).get_client(json)

            example_get_client_flow()
            ```
        """
        credentials = self._get_credentials_from_service_account(self.service_account_json)

        # override class project if method project is provided
        project = project or self.project
        storage_client = Client(credentials=credentials, project=project)
        return storage_client
