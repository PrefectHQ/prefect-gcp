import json
from enum import Enum
from typing import TYPE_CHECKING, Optional

from prefect.infrastructure.docker import DockerRegistry

if TYPE_CHECKING:
    from google.cloud.storage import Bucket

    from prefect_gcp.credentials import GcpCredentials


class LocationType(Enum):
    REGION = "Region"
    MULTI_REGION = "Multi-region"


class GoogleCloudRegistry(DockerRegistry):
    _block_type_name = "Google Cloud Registry"
    credentials: GcpCredentials

    def login(self):
        client = self._get_docker_client()
        with open(self.credentials.service_account_file, "r") as f:
            password = json.load(f)

        return self._login(password=password, client=client)

    def _login(self, password, client):
        return client.login(
            username=self.username,
            password=json.dumps(password),
            registry=self.registry_url,
            reauth=self.reauth,
        )


# class ContainerRegistry(GoogleCloudRegistry):
#     pass

# class ArtifactRegistry(GoogleCloudRegistry):
#     format: str
#     location_type: LocationType
#     region: Optional[str]
#     multi_region: Optional[str]
#     labels: list[str]
