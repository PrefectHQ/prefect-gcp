# Google Cloud Run Worker Guide

## Benefits of Using Google Cloud Run for Flow Run Execution
Google Cloud Run is a fully managed compute platform that automatically scales your containerized applications.
Here are some reasons why you may consider running your flows as Google Cloud Run jobs:

1. Serverless Architecture: Cloud Run follows a serverless architecture, which means you don't need to manage any underlying infrastructure. Google Cloud Run automatically handles the scaling and availability of your pipeline, allowing you to focus on developing and deploying your code.

2. Scalability: Cloud Run can automatically scale your pipeline to handle varying workloads and traffic. It can quickly respond to increased demand and scale back down during low activity periods, ensuring efficient resource utilization.

3. Integration with Google Cloud Services: Google Cloud Run easily integrates with other Google Cloud services, such as Google Cloud Storage, Google Cloud Pub/Sub, and Google Cloud Build. This enables you to build end-to-end data pipelines utilizing a variety of services.

4. Portability: Since Cloud Run uses container images, you can develop your pipelines locally using Docker and then deploy them on Google Cloud Run without significant modifications. This portability allows you to run the same pipeline in different environments.

## Google Cloud Run Guide
After completing this guide, you will have:

1. Created a Google Cloud Service Account
2. Created a Cloud Run Work Pool
3. Deployed a Cloud Run Worker
4. Deployed a Flow
5. Executed the Flow as a Google Cloud Run Job

If you're looking for an introduction to workers, workpools, and deployments, check out the deployment tutorial.

### Prerequisites
Before starting this guide, make sure you have:

- A Google Cloud Platform (GCP) account.
- A project on your GCP account where you have Owner permissions, or atleast permissions to create Cloud Run Services and Service Accounts.
- The `gcloud` CLI installed on your local machine. You can follow Google Cloud's installation guide. If you're using Apple (or a Linux system) you can also use Homebrew for installation.
- Docker installed on your local machine.
- A Prefect server instance. You can sign up for a forever free Prefect Cloud Account or, alternatively, self-host a Prefect server.

### Step 1. Creating a Google Cloud Service Account
First, open a terminal or command prompt on your local machine where `gcloud` is installed. If you haven't already authenticated with `gcloud`, run the following command and follow the instructions to log in to your GCP account.

```bash
gcloud auth login
```

Next, you'll set your project where you'd like to create the service account. Use the ffollowing command and replace `[PROJECT_ID]` with your GCP project's ID.

```bash
gcloud config set project [PROJECT_ID]
```

For example, if your project's ID is `prefect-project` the command will look like this:

```bash
gcloud config set project prefect-project
```

Now you're ready to make the service account. To do so, you'll need to run this command:

```bash
gcloud iam service-accounts create [SERVICE_ACCOUNT_NAME] --display-name="[DISPLAY_NAME]"
```

Here's an example of the command above which you can use which already has the service account name and display name provided. An additional option to describe the service account has also been added:

```bash
gcloud iam service-accounts create prefect-service-account \
    --description="service account to use for the prefect worker" \
    --display-name="prefect-service-account"
```

The last step of this process is to make sure the service account has the proper permissions to execute flow runs as Cloud Run jobs.
Run the following commands to grant the necessary permissions:

```bash
gcloud projects add-iam-policy-binding [PROJECT_ID] \
    --member="serviceAccount:[SERVICE_ACCOUNT_NAME]@[PROJECT_ID].iam.gserviceaccount.com" \
    --role="roles/iam.serviceAccountUser"
```
```bash
gcloud projects add-iam-policy-binding [PROJECT_ID] \
    --member="serviceAccount:[SERVICE_ACCOUNT_NAME]@[PROJECT_ID].iam.gserviceaccount.com" \
    --role="roles/run.admin"
```

### Step 2. Creating a Cloud Run Work Pool

### Step 3. Deploying a Cloud Run Worker

### Step 4. Deploying a Flow

### Step 5. Flow Execution
