# prefect-gcp

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-gcp/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-gcp?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/PrefectHQ/prefect-gcp/" alt="Stars">
        <img src="https://img.shields.io/github/stars/PrefectHQ/prefect-gcp?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-gcp/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-gcp?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/PrefectHQ/prefect-gcp/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/PrefectHQ/prefect-gcp?color=0052FF&labelColor=090422" /></a>
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

To use `prefect-gcp` and Cloud Run:

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

To use Vertex AI:
```bash
pip install "prefect-gcp[aiplatform]"
```

A list of available blocks in `prefect-gcp` and their setup instructions can be found [here](https://prefecthq.github.io/prefect-gcp/#blocks-catalog/).

### Write and run a flow

#### Download blob from bucket

```python
from prefect import flow
from prefect_gcp.cloud_storage import GcsBucket

@flow
def download_flow():
    gcs_bucket = GcsBucket.load("my-bucket")
    path = gcs_bucket.download_object_to_path("my_folder/notes.txt", "notes.txt")
    return path

download_flow()
```

#### Deploy command on Cloud Run

Save the following as `prefect_gcp_flow.py`:

```python
from prefect import flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_run import CloudRunJob

@flow
def cloud_run_job_flow():
    cloud_run_job = CloudRunJob(
        image="us-docker.pkg.dev/cloudrun/container/job:latest",
        credentials=GcpCredentials.load("MY_BLOCK_NAME"),
        region="us-central1",
        command=["echo", "hello world"],
    )
    return cloud_run_job.run()
```

Deploy `prefect_gcp_flow.py`:

```python
from prefect.deployments import Deployment
from prefect_gcp_flow import cloud_run_job_flow

deployment = Deployment.build_from_flow(
    flow=cloud_run_job_flow,
    name="cloud_run_job_deployment", 
    version=1, 
    work_queue_name="demo",
)
deployment.apply()
```

Run the deployment either on the UI or through the CLI:
```bash
prefect deployment run cloud-run-job-flow/cloud_run_job_deployment
```

Visit [Prefect Deployments](https://docs.prefect.io/tutorials/deployments/) for more information about deployments.

#### Get Google auth credentials from GcpCredentials

To instantiate a Google Cloud client, like `bigquery.Client`, `GcpCredentials` is not a valid input. Instead, use the `get_credentials_from_service_account` method.

```python
import google.cloud.bigquery
from prefect import flow
from prefect_gcp import GcpCredentials

@flow
def create_bigquery_client():
    gcp_credentials_block = GcpCredentials.load("BLOCK_NAME")
    google_auth_credentials = gcp_credentials_block.get_credentials_from_service_account()
    bigquery_client = bigquery.Client(credentials=google_auth_credentials)
```

Or simply call `get_bigquery_client` from `GcpCredentials`.

```python
from prefect import flow
from prefect_gcp import GcpCredentials

@flow
def create_bigquery_client():
    gcp_credentials_block = GcpCredentials.load("BLOCK_NAME")
    bigquery_client = gcp_credentials_block.get_bigquery_client()
```

#### Deploy command on Vertex AI as a flow

Save the following as `prefect_gcp_flow.py`:

```python
from prefect import flow
from prefect_gcp.credentials import GcpCredentials
from prefect_gcp.aiplatform import VertexAICustomTrainingJob

@flow
def vertex_ai_job_flow():
    gcp_credentials = GcpCredentials.load("MY_BLOCK")
    job = VertexAICustomTrainingJob(
        command=["echo", "hello world"],
        region="us-east1",
        image="us-docker.pkg.dev/cloudrun/container/job:latest",
        gcp_credentials=gcp_credentials,
    )
    job.run()

vertex_ai_job_flow()
```

Deploy `prefect_gcp_flow.py`:


```python
from prefect.deployments import Deployment
from prefect_gcp_flow import vertex_ai_job_flow

deployment = Deployment.build_from_flow(
    flow=vertex_ai_job_flow,
    name="vertex-ai-job-deployment", 
    version=1, 
    work_queue_name="demo",
)
deployment.apply()
```

Run the deployment either on the UI or through the CLI:
```bash
prefect deployment run vertex-ai-job-flow/vertex-ai-job-deployment
```

Visit [Prefect Deployments](https://docs.prefect.io/tutorials/deployments/) for more information about deployments.

#### Use `with_options` to customize options on any existing task or flow

```python
from prefect import flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import cloud_storage_download_blob_as_bytes

custom_download = cloud_storage_download_blob_as_bytes.with_options(
    name="My custom task name",
    retries=2,
    retry_delay_seconds=10,
)
 
 @flow
 def example_with_options_flow():
    gcp_credentials = GcpCredentials(
        service_account_file="/path/to/service/account/keyfile.json")
    contents = custom_download("bucket", "blob", gcp_credentials)
    return contents()
 
example_with_options_flow()
```
 
For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://orion-docs.prefect.io/collections/usage/)!

## Resources

If you encounter any bugs while using `prefect-gcp`, feel free to open an issue in the [prefect-gcp](https://github.com/PrefectHQ/prefect-gcp) repository.

If you have any questions or issues while using `prefect-gcp`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to ⭐️ or watch [`prefect-gcp`](https://github.com/PrefectHQ/prefect-gcp) for updates too!

## Development

If you'd like to install a version of `prefect-gcp` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-gcp.git

cd prefect-gcp/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
