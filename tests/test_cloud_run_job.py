from http import client
from importlib.metadata import metadata
from unittest import mock
import pytest
from prefect_gcp.cloud_run_job import CloudRunJob 
from prefect_gcp.cloud_run_job.gcp_cloud_run_job import CloudRunJobResult, Execution, Job
from prefect_gcp.credentials import GcpCredentials
from prefect.settings import temporary_settings, PREFECT_API_URL, PREFECT_API_KEY, PREFECT_PROFILES_PATH
from googleapiclient.http import HttpMock
from googleapiclient import discovery
from unittest.mock import Mock
import prefect_gcp


executions_return_value={
    "metadata": {
        "name": "test-name",
        "namespace": "test-namespace"
    },
    "spec": "my-spec",
    "status": {"logUri": "test-log-uri"}
}

jobs_return_value = {
    "metadata": {"name": "Test", "namespace": "test-namespace"},
    "spec": {"MySpec": "spec"},
    "status": {
        "conditions": [{"type": "Ready", "dog": "cat"}],
        "latestCreatedExecution": {"puppy": "kitty"}
    }
}              


@pytest.fixture
def mock_credentials(monkeypatch):
    mock_credentials = Mock(name="Credentials")
    monkeypatch.setattr(
        "prefect_gcp.cloud_run_job.gcp_cloud_run_job.GcpCredentials.get_credentials_from_service_account",
        mock_credentials
    )

    return mock_credentials

@pytest.fixture
def mock_client(monkeypatch, mock_credentials):
    def get_mock_client(*args, **kwargs):
        m = Mock(name="MockClient")
        return m

    monkeypatch.setattr(
        "prefect_gcp.cloud_run_job.gcp_cloud_run_job.CloudRunJob._get_client",
        get_mock_client
    )

    return get_mock_client()


class MockExecution(Mock):
    call_count=0
    def __init__(self, succeeded=False, *args, **kwargs):
        super().__init__()
        self.log_uri = "test_uri"
        self._succeeded = succeeded
    
    def is_running(self):
        MockExecution.call_count += 1

        if self.call_count > 2:
            return False
        return True

    def condition_after_completion(self):
        return {"message": "test"}

    def succeeded(self):
        return self._succeeded

    @classmethod 
    def get(cls, *args, **kwargs):
        return cls()
    

def list_mock_calls(mock_client):
    return [str(call) for call in mock_client.mock_calls]

class TestJob:
    @pytest.mark.parametrize(
        "ready_condition,expected_value",
        [
            ({"status": "True"}, True),
            ({"status": "False"}, False),
            ({}, False)
        ]
    )
    def test_is_ready(self, ready_condition, expected_value):
        job = Job(
            metadata={}, 
            spec={}, 
            status={}, 
            name="test", 
            ready_condition=ready_condition,
            execution_status={}
            )
        assert job.is_ready() == expected_value

    @pytest.mark.parametrize(
        "status,expected_value",
        [
            ({}, {}),
            ({"conditions": []}, {}),
            ({"conditions": [{"type": "Dog", "val": "value"}]}, {}),
            ({"conditions": [{"type": "Ready", "val": "value"}]}, {"type": "Ready", "val": "value"}),
            ({"conditions": [
                {"type": "Dog", "val": "value"},
                {"type": "Ready", "val": "value"}
                ]
            }, 
                {"type": "Ready", "val": "value"})
        ]
    )
    def test_get_ready_condition(self, status, expected_value):
        assert Job._get_ready_condition({"status": status}) == expected_value

    @pytest.mark.parametrize(
        "status,expected_value",
        [
            ({}, {}),
            ({"latestCreatedExecution": {}}, {}),
            ({"latestCreatedExecution": {"some": "val"}}, {"some": "val"}),
        ]
    )
    def test_get_execution_status(self, status, expected_value):
        assert Job._get_execution_status({"status": status}) == expected_value

    @pytest.mark.parametrize(
        "execution_status,expected_value",
        [
            ({}, True), # Has no execution
            ({"completionTimestamp": None}, True), # Has an execution with no completion timestamp
            ({"completionTimestamp": "Exists"}, False), # Has an execution and it has a completion timestamp
        ]
    )
    def test_has_execution_in_progress(self, execution_status, expected_value):
        job = Job(
            metadata={}, 
            spec={}, 
            status={}, 
            name="test", 
            ready_condition={},
            execution_status=execution_status
            )
        assert job.has_execution_in_progress() == expected_value

    def test_get_calls_correct_methods(self, mock_client):
        """
        Desired behavior: should call jobs().get().execute() with correct
        job name and namespace
        """
        mock_client.jobs().get().execute.return_value = jobs_return_value
        Job.get(
            client=mock_client,
            namespace="my-project-id",
            job_name="my-job-name"
        )
        desired_calls = [
            "call.jobs()", # Used to setup mock return
            "call.jobs().get()", # Used to setup mock return
            "call.jobs()",
            "call.jobs().get(name='namespaces/my-project-id/jobs/my-job-name')",
            "call.jobs().get().execute()"
        ]
        actual_calls = list_mock_calls(mock_client=mock_client)
        assert actual_calls == desired_calls

    def test_return_value_for_get(self, mock_client):
        """
        Desired behavior: should a jobs object populated with values from
        `jobs_return_value` test object
        """
        mock_client.jobs().get().execute.return_value = jobs_return_value
        res = Job.get(
            client=mock_client,
            namespace="my-project-id",
            job_name="my-job-name"
        )

        assert res.name == jobs_return_value["metadata"]["name"]
        assert res.metadata == jobs_return_value["metadata"]
        assert res.spec == jobs_return_value["spec"]
        assert res.status == jobs_return_value["status"]
        assert res.ready_condition == jobs_return_value["status"]["conditions"][0]
        assert res.execution_status == jobs_return_value["status"]["latestCreatedExecution"]

    def test_delete_job(self, mock_client):
        """
        Desired behavior: should call jobs().delete().execute() with correct
        job name and namespace        
        """
        Job.delete(
            client=mock_client, 
            namespace='my-project-id',
            job_name='my-job-name'
            )
        desired_calls = [
            "call.jobs()",
            "call.jobs().delete(name='namespaces/my-project-id/jobs/my-job-name')",
            "call.jobs().delete().execute()"
        ]
        actual_calls = list_mock_calls(mock_client=mock_client)
        assert actual_calls == desired_calls

    def test_run_job(self, mock_client):
        """
        Desired behavior: should call jobs().run().execute() with correct
        job name and namespace        
        """
        Job.run(
            client=mock_client, 
            namespace='my-project-id',
            job_name='my-job-name'
            )
        desired_calls = [
            "call.jobs()",
            "call.jobs().run(name='namespaces/my-project-id/jobs/my-job-name')",
            "call.jobs().run().execute()"
        ]
        actual_calls = list_mock_calls(mock_client=mock_client)
        assert actual_calls == desired_calls

    def test_create_job(self, mock_client):
        """
        Desired behavior: should call jobs().create().execute() with correct
        namespace and body
        """
        Job.create(
            client=mock_client, 
            namespace='my-project-id',
            body={"dog": "cat"}
            )
        desired_calls = [
            "call.jobs()",
            "call.jobs().create(parent='namespaces/my-project-id', body={'dog': 'cat'})",
            "call.jobs().create().execute()"
        ]
        actual_calls = list_mock_calls(mock_client=mock_client)
        assert actual_calls == desired_calls
class TestExecution:
    def test_succeeded_responds_true(self):
        execution = Execution(
            name="Test",
            namespace="test-namespace",
            metadata={},
            spec={},
            status={"conditions": [{"type": "Completed", "status": "True"}]},
            log_uri=''
        )
        assert execution.succeeded()

    @pytest.mark.parametrize(
        "conditions",
        [
            [],
            [{"type": "Dog", "status": "True"}],
            [{"type": "Completed", "status": "False"}],

        ]
    )
    def test_succeeded_responds_false(self, conditions):
        execution = Execution(
            name="Test",
            namespace="test-namespace",
            metadata={},
            spec={},
            status={"conditions": conditions},
            log_uri=''
        )
        assert not execution.succeeded()

    @pytest.mark.parametrize(
        "status,expected_value",
        [
            ({}, True),
            ({"completionTime": "xyz"}, False)
        ]
    )
    def test_is_running(self, status, expected_value):
        execution = Execution(
            name="Test",
            namespace="test-namespace",
            metadata={},
            spec={},
            status=status,
            log_uri=''
        )
        assert execution.is_running() == expected_value

    @pytest.mark.parametrize(
        "conditions, expected_value",
        [
            ([], None),
            ([{"type": "Dog", "status": "True"}], None),
            ([{"type": "Completed", "status": "False"}], {"type": "Completed", "status": "False"}),
            ([{"type": "Dog", "status": "True"}, {"type": "Completed", "status": "False"}], {"type": "Completed", "status": "False"}),

        ]
    )
    def test_condition_after_completion_returns_correct_condition(self, conditions, expected_value):
        execution = Execution(
            name="Test",
            namespace="test-namespace",
            metadata={},
            spec={},
            status={"conditions": conditions},
            log_uri=''
        )
        assert execution.condition_after_completion() == expected_value

    def test_get(self, mock_client):
        """Uses response defined in `mock_executions_call`"""
        res = Execution.get(
            client=mock_client, 
            namespace="test-namespace",
            execution_name="test-name"
        )

        assert res.name == "test-name"
        assert res.namespace == "test-namespace"
        assert res.metadata == {"name": "test-name", "namespace": "test-namespace"}
        assert res.spec == "my-spec"
        assert res.status == {"logUri": "test-log-uri"}
        assert res.log_uri == "test-log-uri"


@pytest.fixture
def cloud_run_job():
    return CloudRunJob(
        image="gcr.io//not-a/real-image",
        region="middle-earth2",
        credentials=GcpCredentials(service_account_info='{"hello":"world"}'),
    )

class TestCloudRunJobContainerSettings:
    def test_captures_prefect_env(self, cloud_run_job):
        base_setting = {}
        with temporary_settings(updates={PREFECT_API_KEY: "Dog", PREFECT_API_URL: "Puppy", PREFECT_PROFILES_PATH: "Woof"}):
            result = cloud_run_job._add_container_settings(base_setting)
            assert result["env"] == [
                {'name': 'PREFECT_API_URL', 'value': 'Puppy'}, 
                {'name': 'PREFECT_API_KEY', 'value': 'Dog'},
                {'name': 'PREFECT_PROFILES_PATH', 'value': 'Woof'}
            ]

    def test_adds_job_env(self, cloud_run_job):
        base_setting = {}
        cloud_run_job.env = {"TestVar": "It's Working"}

        with temporary_settings(updates={PREFECT_API_KEY: "Dog", PREFECT_API_URL: "Puppy", PREFECT_PROFILES_PATH: "Woof"}):
            result = cloud_run_job._add_container_settings(base_setting)
            assert result["env"] == [
                {'name': 'PREFECT_API_URL', 'value': 'Puppy'}, 
                {'name': 'PREFECT_API_KEY', 'value': 'Dog'},
                {'name': 'PREFECT_PROFILES_PATH', 'value': 'Woof'},
                {'name': "TestVar", 'value': "It's Working"}
            ]

    def test_job_env_overrides_base_env(self, cloud_run_job):
        base_setting = {}
        cloud_run_job.env = {"TestVar": "It's Working", "PREFECT_API_KEY": "Cat", "PREFECT_API_URL": "Kitty"}

        with temporary_settings(updates={PREFECT_API_KEY: "Dog", PREFECT_API_URL: "Puppy", PREFECT_PROFILES_PATH: "Woof"}):
            result = cloud_run_job._add_container_settings(base_setting)
            assert result["env"] == [
                {'name': 'PREFECT_API_URL', 'value': 'Kitty'}, 
                {'name': 'PREFECT_API_KEY', 'value': 'Cat'},
                {'name': 'PREFECT_PROFILES_PATH', 'value': 'Woof'},
                {'name': "TestVar", 'value': "It's Working"}
            ]

    def test_default_command_is_correct(self, cloud_run_job):
        default_cmd = ['python', '-m', 'prefect.engine']
        assert cloud_run_job.command == default_cmd

        base_setting = {}
        result = cloud_run_job._add_container_settings(base_setting)
        assert result["command"] == default_cmd
    
    def test_command_overrides_default(self, cloud_run_job):
        cmd = ["echo", "howdy!"]
        cloud_run_job.command = cmd 
        base_setting = {}
        result = cloud_run_job._add_container_settings(base_setting)
        assert result["command"] == cmd

    def test_resources_skipped_by_default(self, cloud_run_job):
        base_setting = {}
        result = cloud_run_job._add_container_settings(base_setting)
        assert result.get("resources") is None
        
    def test_resources_added_correctly(self, cloud_run_job):
        cpu = "1234"
        memory = "abc"
        cloud_run_job.cpu = cpu
        cloud_run_job.memory = memory
        base_setting = {}
        result = cloud_run_job._add_container_settings(base_setting)

        assert result["resources"] == {
            "limits": {
                "cpu": cpu,
                "memory": memory
            }
        }

    def test_args_skipped_by_default(self, cloud_run_job):
        base_setting = {}
        result = cloud_run_job._add_container_settings(base_setting)
        assert result.get("args") is None

    def test_args_added_correctly(self, cloud_run_job):
        args = ["a", "b"]
        cloud_run_job.args = args
        base_setting = {}
        result = cloud_run_job._add_container_settings(base_setting)
        assert result["args"] == args



class TestCloudRunJobGCPInteraction:

    def test_get_client_uses_correct_endpoint(self, monkeypatch, mock_credentials, cloud_run_job):
        mock = Mock()
        monkeypatch.setattr(
            "prefect_gcp.cloud_run_job.discovery.build",
            mock
        )
        cloud_run_job._get_client() 

        desired_endpoint = f"https://{cloud_run_job.region}-run.googleapis.com"
        assert mock.call_args[1]["client_options"].api_endpoint == desired_endpoint

    def test_get_jobs_client(self, mock_client, cloud_run_job):
        cloud_run_job._get_jobs_client() 
        assert list_mock_calls(mock_client) == ["call()", "call().jobs()"]

    def test_create_job(self, mock_client, cloud_run_job):
        cloud_run_job._project_id = 'my-project-id'
        cloud_run_job._create_job(jobs_client=mock_client, body="Test")

        mock_client.create.assert_called_with(parent='namespaces/my-project-id', body='Test')


    def test_submit_job_for_execution(self, mock_client, cloud_run_job):
        cloud_run_job._project_id = 'my-project-id'
        cloud_run_job._job_name = 'my-job-name'
        cloud_run_job._submit_job_for_execution(jobs_client=mock_client)

        mock_client.run.assert_called_with(name='namespaces/my-project-id/jobs/my-job-name')

    def test_get_job(self, mock_client, cloud_run_job):
        cloud_run_job._project_id = 'my-project-id'
        cloud_run_job._job_name = 'my-job-name'
        cloud_run_job._get_job(jobs_client=mock_client)

        mock_client.get.assert_called_with(name='namespaces/my-project-id/jobs/my-job-name')



class TestCloudRunJobExecution:
    
    def test_wait_for_job_creation(self, monkeypatch, mock_client, cloud_run_job):
        """`_wait_for_job_creation should loop until job.is_ready() == True.

        Behavior to test: should loop while `is_ready()` is False, and should exit the loop
        when `is_ready()` is True.
        """
        class MockJobInstance():
            def __init__(self, is_ready, *args, **kwargs):
                self._is_ready = is_ready
                self.ready_condition = {}

            def is_ready(self):
                return self._is_ready

        class MockJob(Mock):
            call_count = 0
            is_ready = False

            @classmethod
            def from_json(cls, *args, **kwargs):
                """Return a mock object that is ready on the third loop"""
                if cls.call_count < 2:
                    is_ready=False
                else:
                    is_ready = True
                cls.call_count += 1
                return MockJobInstance(is_ready=is_ready)

        monkeypatch.setattr(
            "prefect_gcp.cloud_run_job.Job",
            MockJob
        )
        cloud_run_job._wait_for_job_creation(client=mock_client, poll_interval=1)
        assert MockJob.call_count == 3

    def test_watch_job_execution(self, monkeypatch, mock_client, cloud_run_job):
        """
        Behavior to test: 
        - should loop while `is_running()` is True
        - should exit the loop when `is_running()` is False
        - should return an Execution
        """

        monkeypatch.setattr(
            "prefect_gcp.cloud_run_job.gcp_cloud_run_job.Execution",
            MockExecution
        )

        res = cloud_run_job._watch_job_execution(
            job_execution=MockExecution(),
            poll_interval=0
            )
        assert res.call_count == 3
        assert isinstance(res, MockExecution)

    @pytest.mark.parametrize(
        "keep_job,succeeded,expected_code",
        [
            (True, True, 0),
            (True, False, 1),
            (False, True, 0),
            (False, False, 1)
        ]
    )
    def test_watch_job_execution_and_get_result(self, monkeypatch, mock_client, cloud_run_job, keep_job, succeeded, expected_code):
        """
        Behavior to test:
        - Returns a succeeded CloudRunJobResult if execution.succeeded()
        - Returns a failed CloudRunJobResult if execution.succeeded() is False
        - In either instance, calls delete_job if keep_job_after_completion is false
        """
        def return_mock_execution(*args, **kwargs):
            """Set succeeded value for our MockExecution instance"""
            return MockExecution(succeeded=succeeded)

        # Set whether or not we should delete the job after completion
        cloud_run_job.keep_job_after_completion = keep_job

        monkeypatch.setattr(
            "prefect_gcp.cloud_run_job.CloudRunJob._watch_job_execution",
            return_mock_execution
        )

        res = cloud_run_job._watch_job_execution_and_get_result(
            client=mock_client, 
            execution=MockExecution(), 
            poll_interval=0
        )

        assert isinstance(res, CloudRunJobResult)
        assert res.identifier == cloud_run_job.job_name
        assert res.status_code == expected_code

        # if keep_job:
        #     # There should be no deletes if the job is being kept
        #     assert any(
        #         "call.delete" in str(call) for call in mock_client.method_calls
        #     ) == False
        # else:
        #     breakpoint()
        #     # The last call should be a delete
        #     assert "call.delete" in str(mock_client.method_calls[-1])
    
    def test_run(self):
        """
        Behavior to test:
        - calls create job
        - waits for job creation
            - if job creation fails
                - if delete
                    - deletes the job
                - otherwise does not delete
            - if job creation succeeds
                - sets task status to started
                - returns function 
        """