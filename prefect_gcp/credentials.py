"""Module handling GCP credentials"""

import os
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Union, Dict

from google.oauth2 import service_account
from google.cloud.storage import Client

from prefect import get_run_logger

@dataclass
class GCPCredentials:
    """
    Dataclass used to manage authentication with GCP. GCP authentication is
    handled via the `boto3` module. Refer to the
    [boto3 docs](https://boto3.amazonGCP.com/v1/documentation/api/latest/guide/credentials.html)
    for more info about the possible credential configurations.
    Args:
        service_account_json: Path to the service account JSON keyfile or the JSON string / dictionary.
    """
    
    service_account_json: Union[Dict[str, str], str, Path]

    def get_client(self):
        """
        Examples:
            Gets a GCP Cloud Storage client from a path.
            ```python
            from prefect import flow
            from prefect_gcp.credentials import GCPCredentials

            @flow()
            def example_get_client_flow():
                service_account_json_path = "~/.secrets/prefect-service-account.json"
                client = GCPCredentials(service_account_json_path).get_client()

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
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/integrations%40prefect-dev-807fd2.iam.gserviceaccount.com"
                }
                client = GCPCredentials(service_account_json).get_client(json)

            test_flow()
            ```
        """
        logger = get_run_logger()

        service_account_json = self.service_account_json
        if isinstance(service_account_json, Path):
            service_account_json = str(service_account_json)

        is_str = isinstance(service_account_json, str)
        if isinstance(service_account_json, dict):
            # if is a JSON dict
            credentials = service_account.Credentials.from_service_account_info(service_account_json)
        elif is_str and "{" in service_account_json:
            # if is a JSON string
            service_account_json = json.loads(service_account_json)
            credentials = service_account.Credentials.from_service_account_info(service_account_json)
        elif is_str:
            # if is a path string
            if "~" in service_account_json:
                service_account_json = os.path.expanduser(service_account_json)
            elif "$HOME" in service_account_json:
                service_account_json = os.path.expandvars(service_account_json)
            if not os.path.exists(service_account_json):
                raise ValueError(f"The provided path to the service account is invalid")
            credentials = service_account.Credentials.from_service_account_file(service_account_json)
        else:
            raise ValueError("Unable to evaluate the service_account_json")

        storage_client = Client(credentials=credentials)
        return storage_client
