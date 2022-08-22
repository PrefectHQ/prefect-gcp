from enum import Enum
from prefect.infrastructure.docker import DockerRegistry
from typing import TYPE_CHECKING, Optional
if TYPE_CHECKING:
    from google.cloud.storage import Bucket

    from prefect_gcp.credentials import GcpCredentials

class LocationType(Enum):
    REGION = "Region"
    MULTI_REGION = "Multi-region"

class GoogleCloudRegistry(DockerRegistry):
    pass

class GoogleArtifactRegistry(DockerRegistry):
    format: str
    location_type: LocationType
    region: Optional[str]
    multi_region: Optional[str]
    labels: list[str]