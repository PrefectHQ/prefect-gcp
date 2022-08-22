import json
from prefect.infrastructure.docker import DockerRegistry
from typing import Any, Optional, Union
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
    image_url: Optional[str] = None
    image_name: Optional[str] = None
    credentials: GcpCredentials
    registry: Optional[Any]

    def validate_something_or_another(self):
        if not self.image_name and not self.image_url:
            raise ValueError("You must provide either an image URL or an image name for a cloud run job.")

    def run(self):
        with self._get_client() as jobs_client:
            # self._register(jobs_client=jobs_client)
            res = self._submit_job_run(jobs_client=jobs_client)
            print(res)

    def register(self):
        client = self._get_client()
        self._register(client)

    def _register(self, jobs_client):
        request = jobs_client.create(
            parent=f"namespaces/{self.project_id}",
            body=self._registration_body(jobs_client)
        )
        response = request.execute()
        return response
    
    def _registration_body(self, client):
        # https://cloud.google.com/run/docs/reference/rest/v1/namespaces.jobs#Job
        body = {
            "apiVersion": 'run.googleapis.com/v1',
            "kind": "Job",
            "metadata": {
                "name": self.job_name+"2",
                "annotations": {
                    "run.googleapis.com/launch-stage": "BETA"
                }
            },
            "spec": { # JobSpec job.spec
                "template": { # ExecutionTemplateSpec
                    "spec": { # ExecutionSpec
                        "template": { # TaskTemplateSpec
                            "spec": { # TaskSpec
                                "containers": [{"image": self.image_url}]
                            }
                        }
                    }
                }
            }
        }
        return body

    def _check_registry(self):
        pass

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
    
class GoogleCloudRegistry(DockerRegistry):
    _block_type_name = "Google Cloud Registry" 
    credentials: GcpCredentials

    def login(self):
        client = self._get_docker_client()
        with open(self.credentials.service_account_file, 'r') as f:
            password = json.load(f)
        
        return self._login(password=password, client=client)
    
    def _login(self, password, client):
        return client.login(
            username = self.username,
            password = json.dumps(password),
            registry = self.registry_url,
            reauth = self.reauth
        )



if __name__ == "__main__":
    creds = GcpCredentials(service_account_file="creds.json")
    registry = GoogleCloudRegistry(
        username='_json_key',
        password='',
        registry_url='gcr.io/helical-bongo-360018',
        credentials=creds
    )
    registry.login()

    job = CloudRunJob(
        job_name="peyton-test",
        project_id="helical-bongo-360018",
        region="us-east1",
        image_url="gcr.io/prefect-dev-cloud2/peytons-test-image:latest",
        # image_name = 'gcr.io/helical-bongo-360018/peytons-test-image',
        credentials=creds,
        registry=registry
    )

    job.register()
    # job.run()


