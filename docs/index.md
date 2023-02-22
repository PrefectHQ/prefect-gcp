# Coordinate and use GCP in your dataflow with `prefect-gcp`

<p align="center">
    <img src="https://user-images.githubusercontent.com/15331990/214915616-6369697e-bc84-400a-a584-845d795a68f2.png">
    <br>
    <a href="https://pypi.python.org/pypi/prefect-gcp/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-gcp?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/PrefectHQ/prefect-gcp/" alt="Stars">
        <img src="https://img.shields.io/github/stars/PrefectHQ/prefect-gcp?color=0052FF&labelColor=090422" /></a>
    <a href="https://pypistats.org/packages/prefect-gcp/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-gcp?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/PrefectHQ/prefect-gcp/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/PrefectHQ/prefect-gcp?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

The `prefect-gcp` collection makes it easy to leverage the capabilities of Google Cloud Platform (GCP) in your flows, featuring support for Vertex AI, Cloud Run, BigQuery, Cloud Storage, and Secret Manager.

## Getting Started

### Saving credentials to a block

You will need to first install [prefect-gcp](#installation) and obtain GCP credentials in order to use `prefect-gcp`.

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
).save("BLOCK-NAME-PLACEHOLDER_PLACEHOLDER")
```

Congrats! You can now easily load the saved block, which holds your credentials:

```python
from prefect_gcp import GcpCredentials
GcpCredentials.load("BLOCK-NAME-PLACEHOLDER_PLACEHOLDER")
```

!!! info "Registering blocks"

    Register blocks in this module to
    [view and edit them](https://docs.prefect.io/ui/blocks/)
    on Prefect Cloud:

    ```bash
    prefect block register -m prefect_gcp
    ```

### Using Prefect with Google Cloud Run

`prefect_gcp` allows you to interact with Google Cloud Run with Prefect flows.

The snippets below show how you can use `prefect_gcp` to run a job on Cloud Run. It uses the `CloudRunJob` block as [Prefect infrastructure](https://docs.prefect.io/concepts/infrastructure/).

#### Build an image

First, find an existing image within the Google Artifact Registry. Ensure it has Python and Prefect installed, or follow the instructions below to set one up.

Create a `Dockerfile`.

```dockerfile
FROM prefecthq/prefect:latest-python3.9
RUN pip install prefect-gcp
```

Then push to the Google Artifact Registry.

```bash
gcloud artifacts repositories create test-example-repository --repository-format=docker --location=us
gcloud auth configure-docker us-docker.pkg.dev
docker build -t us-docker.pkg.dev/<PROJECT-NAME-PLACEHOLDER>/test-example-repository/prefect-gcp:latest-python3.9 .
docker push us-docker.pkg.dev/<PROJECT-NAME-PLACEHOLDER>/test-example-repository/prefect-gcp:latest-python3.9
```

#### Save an infrastructure and storage block

Save a custom infrastructure and storage block by updating the placeholders and executing the following snippet.

```python
from prefect_gcp import GcpCredentials, CloudRunJob, GcsBucket

gcp_credentials = GcpCredentials.load("BLOCK-NAME-PLACEHOLDER")

# must be from GCR and have Python + Prefect
image = "us-docker.pkg.dev/<PROJECT-NAME-PLACEHOLDER>/test-example-repository/prefect-gcp:latest-python3.9"  # noqa

cloud_run_job = CloudRunJob(
    image=image,
    credentials=gcp_credentials,
    region="us-central1",
)
cloud_run_job.save("test-example", overwrite=True)

bucket_name = "cloud-run-job-bucket"
cloud_storage_client = gcp_credentials.get_cloud_storage_client()
cloud_storage_client.create_bucket(bucket_name)
gcs_bucket = GcsBucket(
    bucket=bucket_name,
    gcp_credentials=gcp_credentials,
)
gcs_bucket.save("test-example", overwrite=True)
```

#### Write a flow

Then, find an existing flow or define an arbitrary one to test.

```python
from prefect import flow

@flow(log_prints=True)
def cloud_run_job_flow():
    print("Hello, Prefect!")

if __name__ == "__main__":
    cloud_run_job_flow()
```

#### Create a deployment

If the script was named "cloud_run_job_script.py", build the deployment with the following command.

```bash
prefect deployment build cloud_run_job_script.py:cloud_run_job_flow -n cloud-run-deployment -ib cloud-run-job/test-example -sb gcs-bucket/test-example
```

Make any necessary changes to the YAML, if needed, and apply the deployment.

```bash
prefect deployment apply cloud_run_job_flow-deployment.yaml
```

#### Test the deployment

Start up an agent if you haven't.

```bash
prefect agent start -q 'default'
```

Run the deployment once to test.

```bash
prefect deployment run cloud-run-job-flow/cloud-run-deployment
```

You should see `Hello, Prefect!` eventually logged.

!!! info "No class found for dispatch key"

    If you encounter an error message like `KeyError: "No class found for dispatch key 'cloud-run-job' in registry for type 'Block'."`,
    ensure `prefect-gcp` is installed in the environment that your agent is running!


### Using Prefect with Google Vertex AI

`prefect_gcp` allows you to interact with Google Vertex AI with Prefect flows. Be sure to additionally [install](#installation) the AI Platform extra!

Setting up a Vertex AI job is extremely similar to setting up a Cloud Run Job, but replace `CloudRunJob` with the following snippet.

```python
from prefect_gcp import GcpCredentials, VertexAICustomTrainingJob, GcsBucket

gcp_credentials = GcpCredentials.load("BLOCK-NAME-PLACEHOLDER")

vertex_ai_job = VertexAICustomTrainingJob(
    image="IMAGE-NAME-PLACEHOLDER",  # must be from GCR and have Python + Prefect
    credentials=gcp_credentials,
    region="us-central1",
)
vertex_ai_job.save("test-example")
```

### Using Prefect with Google BigQuery

`prefect_gcp` allows you to interact with Google BigQuery within your Prefect flows. Be sure to additionally [install](#installation) the BigQuery extra!

The provided code snippet shows how you can use `prefect_gcp` to create a new dataset in BigQuery, define a table, insert rows, and fetch data from the table using.

```python
from prefect import flow
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse

@flow
def bigquery_flow():
    all_rows = []
    gcp_credentials = GcpCredentials.load("BLOCK-NAME-PLACEHOLDER")

    client = gcp_credentials.get_bigquery_client()
    client.create_dataset("test_example", exists_ok=True)

    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            "CREATE TABLE IF NOT EXISTS test_example.customers (name STRING, address STRING);"
        )
        warehouse.execute_many(
            "INSERT INTO test_example.customers (name, address) VALUES (%(name)s, %(address)s);",
            seq_of_parameters=[
                {"name": "Marvin", "address": "Highway 42"},
                {"name": "Ford", "address": "Highway 42"},
                {"name": "Unknown", "address": "Highway 42"},
            ],
        )
        while True:
            # Repeated fetch* calls using the same operation will
            # skip re-executing and instead return the next set of results
            new_rows = warehouse.fetch_many("SELECT * FROM test_example.customers", size=2)
            if len(new_rows) == 0:
                break
            all_rows.extend(new_rows)
    return all_rows

bigquery_flow()
```

### Using Prefect with Google Cloud Storage

`prefect_gcp` allows you to interact with Google Cloud Storage within your Prefect flows. Be sure to additionally [install](#installation) the Cloud Storage extra!

The provided code snippet shows how you can use `prefect_gcp` to upload a file to a Google Cloud Storage bucket and download the same file under a different file name.

```python
from pathlib import Path
from prefect import flow
from prefect_gcp import GcpCredentials, GcsBucket


@flow
def cloud_storage_flow():
    # create a dummy file to upload
    file_path = Path("test-example.txt")
    file_path.write_text("Hello, Prefect!")

    gcp_credentials = GcpCredentials.load("BLOCK-NAME-PLACEHOLDER")
    gcs_bucket = GcsBucket(
        bucket="BUCKET-NAME-PLACEHOLDER",
        gcp_credentials=gcp_credentials
    )

    gcs_bucket_path = gcs_bucket.upload_from_path(file_path)
    downloaded_file_path = gcs_bucket.download_object_to_path(
        gcs_bucket_path, "downloaded-test-example.txt"
    )
    return downloaded_file_path.read_text()


cloud_storage_flow()
```

### Using Prefect with Google Secret Manager

`prefect_gcp` allows you to interact with Google Secret Manager within your Prefect flows. Be sure to additionally [install](#installation) the Secret Manager extra!

The provided code snippet shows how you can use `prefect_gcp` to write a secret to the Secret Manager, read the secret data, delete the secret, and finally return the secret data.

```python
from prefect import flow
from prefect_gcp import GcpCredentials, GcpSecret


@flow
def secret_manager_flow():
    gcp_credentials = GcpCredentials.load("BLOCK-NAME-PLACEHOLDER")
    gcp_secret = GcpSecret(secret_name="test-example", gcp_credentials=gcp_credentials)
    gcp_secret.write_secret(secret_data=b"Hello, Prefect!")
    secret_data = gcp_secret.read_secret()
    gcp_secret.delete_secret()
    return secret_data

secret_manager_flow()
```

### Accessing Google credentials or clients from GcpCredentials

In the case that `prefect-gcp` is missing a feature, feel free to [submit an issue](#feedback).

In the meantime, you may want to access the underlying Google credentials or clients, which `prefect-gcp` exposes straightforwardly.

The provided code snippet shows how you can use `prefect_gcp` to instantiate a Google Cloud client, like `bigquery.Client`.

Note a `GcpCredentials` object is NOT a valid input to the underlying BigQuery client--use the `get_credentials_from_service_account` method to access and pass an actual `google.auth.Credentials` object.

```python
import google.cloud.bigquery
from prefect import flow
from prefect_gcp import GcpCredentials

@flow
def create_bigquery_client():
    gcp_credentials = GcpCredentials.load("BLOCK-NAME-PLACEHOLDER")
    google_auth_credentials = gcp_credentials.get_credentials_from_service_account()
    bigquery_client = bigquery.Client(credentials=google_auth_credentials)
```

If you simply want to access the underlying client, `prefect-gcp` exposes a `get_client` method from `GcpCredentials`.

```python
from prefect import flow
from prefect_gcp import GcpCredentials

@flow
def create_bigquery_client():
    gcp_credentials = GcpCredentials.load("BLOCK-NAME-PLACEHOLDER")
    bigquery_client = gcp_credentials.get_client("bigquery")
```

## Resources

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://docs.prefect.io/collections/usage/)!

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

A list of available blocks in `prefect-gcp` and their setup instructions can be found [here](https://prefecthq.github.io/prefect-gcp/blocks_catalog).

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2. For more information about how to use Prefect, please refer to the [Prefect documentation](https://docs.prefect.io/).

### Feedback

If you encounter any bugs while using `prefect-gcp`, feel free to open an issue in the [`prefect-gcp`](https://github.com/PrefectHQ/prefect-gcp) repository.

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
