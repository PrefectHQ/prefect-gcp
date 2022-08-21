from typing import Any, Optional
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from google.oauth2 import service_account
from prefect.blocks.core import Block
from prefect_gcp.credentials import GcpCredentials

# scopes = ['https://www.googleapis.com/auth/cloud-platform']
# sa_file = 'creds.json'
# region = 'us-east1'
# project_id = 'helical-bongo-360018' # Project ID, not Project Name

# api_endpoint = f"https://{region}-run.googleapis.com"
# options = ClientOptions(
#     api_endpoint=api_endpoint
# )

# credentials = service_account.Credentials.from_service_account_file(sa_file, scopes=scopes)

# # Create the Cloud Compute Engine service object
# service = discovery.build('run', 'v1', client_options=options, credentials=credentials)
# breakpoint()
# request = service.namespaces().jobs().list(parent=f'namespaces/helical-bongo-360018')
# response = request.execute()
# breakpoint()
# request = service.namespaces().jobs().run(name='namespaces/helical-bongo-360018/jobs/peyton-test')
# response = request.execute()
# print(response)
# breakpoint()
class CloudRunJobSetting(Block):
    pass
class CloudRunJob(Infrastructure):
    job_name: str
    type: str = "Cloud Run Job"
    project_id: str
    region: str
    image_url: str
    credentials: GcpCredentials
    registry: Optional[Any]

    def run(self):
        with self._get_client() as jobs_client:
            # self._register(jobs_client=jobs_client)
            res = self._submit_job_run(jobs_client=jobs_client)
            print(res)

    def _register(self, jobs_client):
        request = jobs_client.create(...)
        # gcloud beta run jobs create <job_name> --image <self.image_url> --project <self.project>

    def _submit_job_run(self, jobs_client):
        request = jobs_client.run(
            name=f"namespaces/{self.project_id}/jobs/{self.job_name}"
        )
        response = request.execute()
        return response

    def _get_client(self):
        # region needed for 'v1' API
        api_endpoint = f"https://{self.region}-run.googleapis.com"
        credentials = self.credentials.get_credentials_from_service_account()
        options = ClientOptions(api_endpoint=api_endpoint)

        return discovery.build(
            'run', 
            'v1',
            client_options=options,
            credentials=credentials
            ).namespaces().jobs()

    def preview(self):
        pass
if __name__ == "__main__":
    creds = GcpCredentials(service_account_file="creds.json")

    job = CloudRunJob(
        job_name="peyton-test",
        project_id="helical-bongo-360018",
        region="us-east1",
        image_url="gcr.io/prefect-dev-cloud2/peytons-test-image:latest",
        credentials=creds,

    )

    job.run()


