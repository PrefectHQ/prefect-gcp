from prefect._internal.compatibility.deprecated import (
    register_renamed_module,
)

from . import _version
from .aiplatform import VertexAICustomTrainingJob  # noqa
from .bigquery import BigQueryWarehouse  # noqa
from .cloud_run import CloudRunJob  # noqa
from .cloud_storage import GcsBucket  # noqa
from .credentials import GcpCredentials  # noqa
from .secret_manager import GcpSecret  # noqa
from .vertex_worker import VertexAIWorker  # noqa
from .worker import CloudRunWorker  # noqa

register_renamed_module(
    "prefect_gcp.projects", "prefect_gcp.deployments", start_date="Jun 2023"
)


__version__ = _version.get_versions()["version"]
