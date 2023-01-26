# Coordinate and use GCP in your dataflow with `prefect-gcp`

<p align="center">
    <img src="https://user-images.githubusercontent.com/15331990/214915616-6369697e-bc84-400a-a584-845d795a68f2.png">
    <br>
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

Visit the full docs [here](https://PrefectHQ.github.io/prefect-gcp) to see additional examples and the API reference.

The `prefect-openai` collection makes it easy to leverage the capabilities of Google Cloud Platform (GCP) in your flows. Check out the examples below to get started!

## Getting Started

### Saving credentials to a block

You will need to obtain GCP credentials in order to use `prefect-gcp`.

1. Refer to the [GCP service account documentation](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating) on how to create and download a service account key file
2. Copy the JSON contents
3. Create a short script, replacing the placeholders (or do so in the UI)

```python
from prefect_gcp import GcpCredentials

# replace this PLACEHOLDER dict with your own service account info
service_account_info = {
  "type": "service_account",
  "project_id": "PROJECT_ID",
  "private_key_id": "KEY_ID",
  "private_key": "-----BEGIN PRIVATE KEY-----\nPRIVATE_KEY\n-----END PRIVATE KEY-----\n",
  "client_email": "SERVICE_ACCOUNT_EMAIL",
  "client_id": "CLIENT_ID",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/SERVICE_ACCOUNT_EMAIL"
}

GcpCredentials(
    service_account_info=service_account_info
).save("BLOCK_NAME_PLACEHOLDER")
```

Congrats! You can now easily load the saved block, which holds your credentials:

```python
from prefect_gcp import GcpCredentials
GcpCredentials.load("BLOCK_NAME_PLACEHOLDER")
```

!!! info Registering blocks

    Register blocks in this module to
    [view and edit them](https://orion-docs.prefect.io/ui/blocks/)
    on Prefect Cloud:

    ```bash
    prefect block register -m prefect_gcp
    ```

### Download blob from bucket

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

### Deploy command on Cloud Run

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

### Get Google auth credentials from GcpCredentials

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

### Deploy command on Vertex AI as a flow

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

## Resources

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://orion-docs.prefect.io/collections/usage/)!

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

A list of available blocks in `prefect-gcp` and their setup instructions can be found [here](https://prefecthq.github.io/prefect-gcp/#blocks-catalog).

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Feedback

If you encounter any bugs while using `prefect-gcp`, feel free to open an issue in the [prefect-gcp](https://github.com/PrefectHQ/prefect-gcp) repository.

If you have any questions or issues while using `prefect-gcp`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to star or watch [`prefect-gcp`](https://github.com/PrefectHQ/prefect-gcp) for updates too!

### Contributing

If you'd like to help contribute to fix an issue or add a feature to `prefect-gcp`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

Here are the steps:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes
5. Add tests
6. Insert an entry to [CHANGELOG.md](https://github.com/PrefectHQ/prefect-gcp/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request
