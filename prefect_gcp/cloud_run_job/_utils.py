from googleapiclient.discovery import Resource
from pydantic import BaseModel

class Job(BaseModel):
    metadata: dict
    spec: dict
    status: dict
    name: str
    ready_condition: dict
    execution_status: dict

    def _is_missing_container(self):
        """Check if Job status is not ready because the specified container cannot be found."""
        if (
            self.ready_condition.get("status") == "False"
            and self.ready_condition.get("reason") == "ContainerMissing"
        ):
            return True
        return False

    def is_ready(self) -> bool:
        """Whether a job is finished registering and ready to be executed"""
        if self._is_missing_container():
            raise Exception(f"{self.ready_condition['message']}")
        return self.ready_condition.get("status") == "True"

    def has_execution_in_progress(self) -> bool:
        """See if job has a run in progress."""
        return (
            self.execution_status == {}
            or self.execution_status.get("completionTimestamp") is None
        )

    @staticmethod
    def _get_ready_condition(job: dict) -> dict:
        if job["status"].get("conditions"):
            for condition in job["status"]["conditions"]:
                if condition["type"] == "Ready":
                    return condition

        return {}

    @staticmethod
    def _get_execution_status(job: dict):
        if job["status"].get("latestCreatedExecution"):
            return job["status"]["latestCreatedExecution"]

        return {}

    @classmethod
    def get(cls, client: Resource, namespace:str, job_name:str):
        request = client.jobs().get(
            name=f"namespaces/{namespace}/jobs/{job_name}"
        )
        response = request.execute()

        return cls(
            metadata=response["metadata"],
            spec=response["spec"],
            status=response["status"],
            name=response["metadata"]["name"],
            ready_condition=cls._get_ready_condition(response),
            execution_status=cls._get_execution_status(response),
        )

    @classmethod
    def create(cls, client: Resource, namespace:str, body:dict):
        request = client.jobs().create(parent=f"namespaces/{namespace}", body=body)
        response = request.execute()
        return response

    @classmethod
    def delete(cls, client: Resource, namespace:str, job_name:str):
        request = client.jobs().delete(
            name=f"namespaces/{namespace}/jobs/{job_name}"
        )
        response = request.execute()
        return response
    
    @classmethod
    def run(cls, client: Resource, namespace:str, job_name:str):
        request = client.jobs().run(
            name=f"namespaces/{namespace}/jobs/{job_name}"
        )
        response = request.execute()
        return response


class Execution(BaseModel):
    name: str
    namespace: str
    metadata: dict
    spec: dict
    status: dict
    log_uri: str

    def is_running(self) -> bool:
        """Returns True if Execution is not completed."""
        return self.status.get("completionTime") is None

    def condition_after_completion(self):
        """Returns Execution condition if Execution has completed."""
        for condition in self.status["conditions"]:
            if condition["type"] == "Completed":
                return condition

    def succeeded(self):
        """Whether or not the Execution completed is a successful state."""
        completed_condition = self.condition_after_completion()
        if completed_condition and completed_condition["status"] == "True":
            return True

        return False

    @classmethod
    def get(cls, client, namespace, execution_name):
        request = client.executions().get(
            # name=f"namespaces/{job_execution.metadata['namespace']}/executions/{job_execution.metadata['name']}"
            name=f"namespaces/{namespace}/executions/{execution_name}"
        )
        response = request.execute()

        return cls(
            name=response["metadata"]["name"],
            namespace=response["metadata"]["namespace"],
            metadata=response["metadata"],
            spec=response["spec"],
            status=response["status"],
            log_uri=response["status"]["logUri"],
        )
