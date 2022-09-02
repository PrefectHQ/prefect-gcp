from importlib.metadata import metadata
import pytest
from prefect_gcp.cloud_run_job import CloudRunJob, Execution, Job
from prefect_gcp.credentials import GcpCredentials
from prefect.settings import temporary_settings, PREFECT_API_URL, PREFECT_API_KEY, PREFECT_PROFILES_PATH
from googleapiclient.http import HttpMock
from googleapiclient import discovery
from unittest.mock import Mock
import prefect_gcp

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

    @pytest.mark.parametrize(
        "condition,execution,expected_condition,expected_ex_status",
        [
            (   # Nothing -> nothing
                None,
                None,
                {},
                {}
            ),
            (   # Nothing -> nothing
                {},
                {},
                {},
                {}
            ),
            (   # Empty conditions
                {"conditions": []},
                {},
                {},
                {}
            ),
            (   # Ready status -> ready status
                {"conditions": [{"type": "Ready", "dog": "cat"}]},
                {},
                {"type": "Ready", "dog": "cat"},
                {}
            ),
            (   # Ready status and execution -> ready status and execution
                {"conditions":[{"type": "Ready", "dog": "cat"}]},
                {"latestCreatedExecution": {"puppy": "kitty"}},
                {"type": "Ready", "dog": "cat"},
                {"puppy": "kitty"}
            ),
            (   # Other status and execution -> nothing and execution
                {"conditions": [{"type": "OtherThing", "dog": "cat"}]},
                {"latestCreatedExecution": {"puppy": "kitty"}},
                {},
                {"puppy": "kitty"}
            ),
            (   # multiple status items -> ready status
                {"conditions":[
                    {"type": "OtherThing", "dog": "cat"},
                    {"type": "Ready", "dog": "cat"}
                    ]},
                {"latestCreatedExecution": {"puppy": "kitty"}},
                {"type": "Ready", "dog": "cat"},
                {"puppy": "kitty"}
            ),
        ]
    )
    def test_from_json(self, condition, execution, expected_condition, expected_ex_status):
        status = {}
        if condition is not None:
            status = {**status, **condition}
        if execution is not None:
            status = {**status, **execution}

        job_dict = {
            "metadata": {"name": "Test"},
            "spec": {"MySpec": "spec"},
            "status": status        
        }
        job = Job.from_json(job_dict)

        assert job.metadata == job_dict["metadata"]
        assert job.spec == job_dict["spec"]
        assert job.name == job_dict["metadata"]["name"]
        assert job.status == job_dict["status"]
        assert job.ready_condition == expected_condition
        assert job.execution_status == expected_ex_status

class TestExecution:
    def test_succeeded_responds_true(self):
        execution = Execution(
            name="Test",
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
            metadata={},
            spec={},
            status={"conditions": conditions},
            log_uri=''
        )
        assert execution.condition_after_completion() == expected_value

    def test_from_json(self):
        execution_dict = {
            "metadata": {"name": "Test"},
            "spec": {"MySpec": "spec"},
            "status": {'logUri': "my_uri.com"}
        }
        execution = Execution.from_json(execution_dict)

        assert execution.name == execution_dict["metadata"]["name"]
        assert execution.metadata == execution_dict["metadata"]
        assert execution.spec == execution_dict["spec"]
        assert execution.status == execution_dict["status"]
        assert execution.log_uri == execution_dict["status"]["logUri"]


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

@pytest.fixture
def mock_credentials(monkeypatch):
    mock_credentials = Mock(name="Credentials")
    monkeypatch.setattr(
        "prefect_gcp.cloud_run_job.GcpCredentials.get_credentials_from_service_account",
        mock_credentials
    )

    return mock_credentials

@pytest.fixture
def mock_client(monkeypatch, mock_credentials):
    mock_client = Mock(name="Client")
    monkeypatch.setattr(
        "prefect_gcp.cloud_run_job.CloudRunJob._get_client",
        mock_client
    )
    return mock_client

def list_mock_calls(mock_client):
    return [str(call) for call in mock_client.mock_calls]

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

    def test_get_executions_client(self, mock_client, cloud_run_job):
        cloud_run_job._get_executions_client() 
        assert list_mock_calls(mock_client) == ["call()", "call().executions()"]
        
    def test_create_job(self, mock_client, cloud_run_job):
        cloud_run_job._project_id = 'my-project-id'
        cloud_run_job._create_job(jobs_client=mock_client, body="Test")

        mock_client.create.assert_called_with(parent='namespaces/my-project-id', body='Test')

    def test_delete_job(self, mock_client, cloud_run_job):
        cloud_run_job._project_id = 'my-project-id'
        cloud_run_job._job_name = 'my-job-name'
        cloud_run_job._delete_job(jobs_client=mock_client)

        mock_client.delete.assert_called_with(name='namespaces/my-project-id/jobs/my-job-name')

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

    def test_get_execution(self, mock_client, cloud_run_job):
        cloud_run_job._project_id = 'my-project-id' # Neither of these will be used
        cloud_run_job._job_name = 'my-job-name'     # because data gather from Execution
        job_execution = Mock(metadata={"namespace": "dog", "name": "puppy"})

        cloud_run_job._get_execution(
            executions_client=mock_client,
            job_execution=job_execution
            )

        mock_client.get.assert_called_with(name='namespaces/dog/executions/puppy')


