import json
from pydantic import Field, BaseModel
from enum import Enum
from typing import TYPE_CHECKING, Optional
from prefect_gcp.credentials import GcpCredentials
from prefect.docker import get_prefect_image_name
from prefect.infrastructure.docker import BaseDockerLogin
import requests

if TYPE_CHECKING:
    from google.cloud.storage import Bucket

    from prefect_gcp.credentials import GcpCredentials

PREFECT_DOCKERHUB_NAME = get_prefect_image_name()

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
    _registry_name: str = None

    def login(self):
        client = self._get_docker_client()
        res = self._login(username=self.username, password=self.password, client=client)

    @property 
    def password(self):
        service_account_value = self.credentials.get_service_account_value()
        if isinstance(service_account_value, dict):
            password = json.dumps(service_account_value)
        else:
            password = service_account_value
        
        return password

    @property
    def username(self):
        return "_json_key"

    def get_prefect_image(self):
        self.login()
        client = self._get_docker_client()
        prefect_image_tag = self._get_tag_for_prefect_image_in_google_registry()
        if prefect_image_tag is None:
            prefect_image_tag = self._add_prefect_image_to_registry(client=client)
        
        return prefect_image_tag

    def _add_prefect_image_to_registry(self, client):
        local_prefect_image = self._get_local_prefect_image()
        if not local_prefect_image:
            try:
                client.images.pull(PREFECT_DOCKERHUB_NAME)
                local_prefect_image = self._get_local_prefect_image()
            except Exception as exc:
                raise
        
        local_prefect_image.tag(self.prefect_gcr_name)
        for line in client.images.push(
            repository=self.prefect_gcr_name, 
            stream=True, 
            auth_config={"username": self.username, "password": self.password},
            decode=True
        ):
            print(line)

        return self.prefect_gcr_name


    def _get_local_prefect_image(self):
        for image in self.list_images():
            for tag in image.tags:
                if tag == PREFECT_DOCKERHUB_NAME:
                    return image

    def _login(self, username, password, client):
        return client.login(
            username=username,
            password=password,
            registry=self.registry_name,
            reauth=True,
        )

    @property
    def registry_name(self):
        if self._registry_name is None:
            self._registry_name = f"gcr.io/{self.credentials.get_project_id()}" 

        return self._registry_name

    @property
    def prefect_gcr_name(self):
        return f"{self.registry_name}/{PREFECT_DOCKERHUB_NAME}"

    def list_images(self):
        access_token = self.credentials.get_access_token()
        resp = requests.get('https://gcr.io/v2/_catalog', auth=('_token', access_token))

        return resp.json()
    
    def _get_tag_for_prefect_image_in_google_registry(self):
        for image in self.list_images():
            for tag in image.tags:
                if self.prefect_gcr_name in tag:
                    return tag


if __name__ == "__main__":
    creds = GcpCredentials(service_account_file="creds.json")
    registry = GoogleCloudRegistry(
        credentials=creds,
    )
    registry.login()
    print(registry.list_images())

# class ContainerRegistry(GoogleCloudRegistry):
#     pass

# class ArtifactRegistry(GoogleCloudRegistry):
#     format: str
#     location_type: LocationType
#     region: Optional[str]
#     multi_region: Optional[str]
#     labels: list[str]
