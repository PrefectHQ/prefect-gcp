# prefect-gcp

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-gcp/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-gcp?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/prefecthq/prefect-gcp/" alt="Stars">
        <img src="https://img.shields.io/github/stars/prefecthq/prefect-gcp?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-gcp/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-gcp?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/prefecthq/prefect-gcp/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/prefecthq/prefect-gcp?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

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

To use Cloud Storage:
```bash
pip install "prefect-gcp[cloud_storage]"
```

To use BigQuery:

```bash
pip install "prefect-gcp[bigquery]"
```

To use Secret Manager:
```bash
pip install "prefect-gcp[secret_manager]"
```

### Write and run a flow

```python
from prefect import flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import cloud_storage_download_blob_as_bytes

@flow()
def example_cloud_storage_download_blob_flow():
    gcp_credentials = GcpCredentials(
        service_account_file="/path/to/service/account/keyfile.json")
    contents = cloud_storage_download_blob_as_bytes("bucket", "blob", gcp_credentials)
    return contents

example_cloud_storage_download_blob_flow()
```

## Resources

If you encounter any bugs while using `prefect-gcp`, feel free to open an issue in the [prefect-gcp](https://github.com/PrefectHQ/prefect-gcp) repository.

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
