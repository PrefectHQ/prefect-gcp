from . import _version

try:
    from .bigquery import BigQueryWarehouse  # noqa
except ImportError:
    pass
try:
    from .aiplatform import VertexAICustomTrainingJob  # noqa
except ImportError:
    pass
try:
    from .cloud_storage import CloudStorage  # noqa
except ImportError:
    pass
try:
    from .cloud_run import CloudRun  # noqa
except ImportError:
    pass
try:
    from .secret_manager import SecretManager  # noqa
except ImportError:
    pass
try:
    from .credentials import GcpCredentials  # noqa
except ImportError:
    pass

__version__ = _version.get_versions()["version"]
