# prefect-gcp

## Welcome!

`prefect-gcp` is a collection of prebuilt Prefect tasks that can be used to quickly construct Prefect flows.

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-gcp` with `pip`:

```bash
pip install prefect-gcp
```

### Write and run a flow

```python
from prefect import flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import cloud_storage_download_blob

@flow()
def example_cloud_storage_download_blob_flow():
    gcp_credentials = GcpCredentials(
        service_account_file="/path/to/service/account/keyfile.json")
    contents = cloud_storage_download_blob("bucket", "blob", gcp_credentials)
    return contents

example_cloud_storage_download_blob_flow()
```

## Resources

If you encounter and bugs while using `prefect-gcp`, feel free to open an issue in the [prefect-gcp](https://github.com/PrefectHQ/prefect-gcp) repository.

If you have any questions or issues while using `prefect-gcp`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-gcp` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-gcp.git

cd prefect-gcp/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
