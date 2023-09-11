from . import _version

from .bigquery import BigQueryWarehouse  # noqa
from .aiplatform import VertexAICustomTrainingJob  # noqa
from .cloud_storage import GcsBucket  # noqa
from .cloud_run import CloudRunJob  # noqa
from .cloud_run_v2 import CloudRunJobV2  # noqa
from .secret_manager import GcpSecret  # noqa
from .credentials import GcpCredentials  # noqa
from .worker import CloudRunWorker  # noqa
from .worker_v2 import CloudRunWorkerV2  # noqa
from prefect._internal.compatibility.deprecated import (
    register_renamed_module,
)

register_renamed_module(
    "prefect_gcp.projects", "prefect_gcp.deployments", start_date="Jun 2023"
)


__version__ = _version.get_versions()["version"]
