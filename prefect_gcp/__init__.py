from . import _version
from .credentials import GcpCredentials  # noqa
from .cloud_run_job import gcp_cloud_run_job as cloud_run_job  # noqa

__version__ = _version.get_versions()["version"]
