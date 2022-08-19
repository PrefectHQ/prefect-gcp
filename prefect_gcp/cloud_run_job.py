from prefect.infrastructure.base import Infrastructure, InfrastructureResult

from googleapiclient import discovery
from google.oauth2 import service_account

from prefect_gcp.credentials import GcpCredentials

scopes = ['https://www.googleapis.com/auth/cloud-platform']
sa_file = 'creds.json'
zone = 'us-central1-a'
project_id = 'prefect-dev-cloud2' # Project ID, not Project Name

credentials = service_account.Credentials.from_service_account_file(sa_file, scopes=scopes)

# Create the Cloud Compute Engine service object
service = discovery.build('compute', 'v1', credentials=credentials)
with discovery.build('run', 'v1') as client:
    # request = client.namespaces().jobs().list(parent=f'namespaces/prefect-dev-cloud2')
    request = client.namespaces().jobs().run(name='namespaces/prefect-dev-cloud2/jobs/peyton-test')
    response = request.execute()
    print(response)

class CloudRunJob(Infrastructure):
    job_name: str
    project_id: str
    zone: str
    image_url: str
    credentials: GcpCredentials

    def run(self):
        with self._get_client() as jobs_client:
            self._register(jobs_client=jobs_client)
            self._submit_job_run(jobs_client=jobs_client)


    def _register(self, jobs_client):
        request = jobs_client.create(...)
        # gcloud beta run jobs create <job_name> --image <self.image_url> --project <self.project>

    def _submit_job_run(self, jobs_client):
        request = jobs_client.run(...)

    def _get_client(self):
        credentials = self.credentials.get_credentials_from_service_account()
        return discovery.build('run', 'v1', credentials=credentials).namespaces().jobs()

if __name__ == "__main__":
    creds = GcpCredentials(service_account_file="creds.json")

    job = CloudRunJob(
        job_name="peyton-test",
        project_id="prefect-dev-cloud2",
        zone="us-east1",
        image_url="gcr.io/prefect-dev-cloud2/peytons-test-image:latest"
    )


