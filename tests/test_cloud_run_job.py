import pytest
from prefect_gcp.cloud_run_job import CloudRunJob
from prefect_gcp.credentials import GcpCredentials
from prefect.settings import temporary_settings, PREFECT_API_URL, PREFECT_API_KEY, PREFECT_PROFILES_PATH

class TestJob:
    def test_get_execution_status(self):
        pass

    def test_get_ready_condition(self):
        pass

    def test_is_ready(self):
        pass

    def test_is_finished(self):
        pass

    def test_from_json(self):
        pass
    

class TestExecution:
    pass

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