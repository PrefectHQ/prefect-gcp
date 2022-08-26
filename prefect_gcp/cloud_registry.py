import json
from pydantic import Field, BaseModel
from enum import Enum
from typing import TYPE_CHECKING, Optional
from prefect_gcp.credentials import GcpCredentials
from prefect.infrastructure.docker import BaseDockerLogin

if TYPE_CHECKING:
    from google.cloud.storage import Bucket

    from prefect_gcp.credentials import GcpCredentials


class LocationType(Enum):
    REGION = "Region"
    MULTI_REGION = "Multi-region"


class GoogleCloudRegistry(BaseDockerLogin):
    _block_type_name = "Google Cloud Registry"
    # registry_url: str = Field(
    #     ...,
    #     description='The URL to the registry. Generally, "http" or "https" can be omitted.',
    # )
    credentials: GcpCredentials
    reauth: bool = Field(
        True, description="Whether or not to reauthenticate on each interaction."
    )
    def login(self):
        client = self._get_docker_client()
        service_account_value = self.credentials.get_service_account_value()
        if isinstance(service_account_value, dict):
            # make fail
            # for k in service_account_value.keys():
            #     service_account_value[k] = "fail"
            password = json.dumps(service_account_value)
        else:
            password = service_account_value
        # return self._login(password=password, client=client)
        res = self._login(password=password, client=client)
        print(res)
        print(client.containers.list())
        # breakpoint()
        # print(client)

    def _login(self, password, client):
        return client.login(
            username="_json_key",
            password=password,
            registry=f"gcr.io/{self.credentials.get_project_id()}",
            reauth=self.reauth,
        )

    def list(self):
        client = self._get_docker_client()
 
if __name__ == "__main__":
    creds = GcpCredentials(service_account_file="creds.json")
    registry = GoogleCloudRegistry(
        credentials=creds,
    )
    registry.login()

# class ContainerRegistry(GoogleCloudRegistry):
#     pass

# class ArtifactRegistry(GoogleCloudRegistry):
#     format: str
#     location_type: LocationType
#     region: Optional[str]
#     multi_region: Optional[str]
#     labels: list[str]
