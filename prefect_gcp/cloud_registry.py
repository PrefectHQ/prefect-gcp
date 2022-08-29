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


class LocationType(Enum):
    REGION = "Region"
    MULTI_REGION = "Multi-region"


class GoogleCloudRegistry(BaseDockerLogin):
    _block_type_name = "Google Cloud Registry"
    credentials: GcpCredentials
    reauth: bool = Field(
        True, description="Whether or not to reauthenticate on each interaction."
    )
    _registry_name: str = None

    @property 
    def password(self):
        """
        Returns the JSON key value from a JSON service account file.
        """
        service_account_value = self.credentials.get_service_account_value()
        if isinstance(service_account_value, dict):
            password = json.dumps(service_account_value)
        else:
            password = service_account_value
        
        return password

    @property
    def username(self):
        """Expected username when using the JSON credential value as a password"""
        return "_json_key" 

    @property
    def registry_name(self):
        """The name of the Google Cloud Registry"""
        if self._registry_name is None:
            self._registry_name = f"{self.credentials.get_project_id()}" 

        return self._registry_name

    @property
    def prefect_gcr_name(self):
        return f"{self.registry_name}/{get_prefect_image_name()}"

    def login(self):
        client = self._get_docker_client()
        return self._login(username=self.username, password=self.password, client=client)

    def _login(self, username, password, client):
        return client.login(
            username=username,
            password=password,
            registry=f"gcr.io/{self.registry_name}",
            reauth=True,
        )

    def get_prefect_image(self):
        """Return the name of the Prefect image in GCR.
        
        If the image does not exist in GCR, this will add the image to GCR and
        return the name of the newly pushed image. 
        """
        self.login()
        client = self._get_docker_client()
        prefect_repository, tag = self.prefect_gcr_name.split(":")
        repos = self._list_repositories()

        # See if an existing match is up there
        if prefect_repository in repos:
            if tag in self._list_tags_for_repository(prefect_repository):
                return self.prefect_gcr_name

        # Add if existing match not there
        self._add_prefect_image_to_registry(client=client)
        return self.prefect_gcr_name

    def _add_prefect_image_to_registry(self, client):
        """Add the correct Prefect docker image to a Google Container Registry.

        This will pull the image from Dockerhub if it does not already exist locally,
        and then push the image Google Container Registry. Directly adding to GCR is
        not an option.
        """
        prefect_image = self._get_local_prefect_image()
        if prefect_image is None:
            try:
                prefect_image = client.images.pull(get_prefect_image_name())
            except Exception as exc:
                raise
        
        destination_tag = f"gcr.io/{self.prefect_gcr_name}" 

        prefect_image.tag(destination_tag)
        for line in client.images.push(
            repository=destination_tag, 
            stream=True, 
            auth_config={"username": self.username, "password": self.password},
            decode=True
        ):
            print(line)

        return self.prefect_gcr_name

    def _list_local_images(self):
        """List all images on the local machine."""
        client = self._get_docker_client()
        images = client.images.list()

        return images

    def _list_repositories(self):
        """List all GCR repositories associated with the credential."""
        access_token = self.credentials.get_access_token()
        resp = requests.get('https://gcr.io/v2/_catalog', auth=('_token', access_token))
        return resp.json()["repositories"]
    
    def _list_tags_for_repository(self, repository):
        """List all tags for a given repository."""
        access_token = self.credentials.get_access_token()
        resp = requests.get(
            f"https://gcr.io/v2/{repository}/tags/list", 
            auth=('_token', access_token)
        )
        tags = resp.json()["tags"] 
        if tags == []:
            raise Exception(f"No repository matching name {repository} was found")
        return tags 

    def _get_local_prefect_image(self):
        """Get an image object for the expected Prefect image."""
        client = self._get_docker_client()
        if get_prefect_image_name() not in self._list_local_images():
            try:
                local_prefect_image = client.images.pull(get_prefect_image_name())
                return local_prefect_image
            except Exception as exc:
                raise 
        return None


if __name__ == "__main__":
    creds = GcpCredentials(service_account_file="creds.json")
    registry = GoogleCloudRegistry(
        credentials=creds,
    )

    registry.get_prefect_image()

# class ContainerRegistry(GoogleCloudRegistry):
#     pass

# class ArtifactRegistry(GoogleCloudRegistry):
#     format: str
#     location_type: LocationType
#     region: Optional[str]
#     multi_region: Optional[str]
#     labels: list[str]
