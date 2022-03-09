from prefect import flow

from prefect_gcp.tasks import (
    goodbye_prefect_gcp,
    hello_prefect_gcp,
)


def test_hello_prefect_gcp():
    @flow
    def test_flow():
        return hello_prefect_gcp()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Hello, prefect-gcp!"


def goodbye_hello_prefect_gcp():
    @flow
    def test_flow():
        return goodbye_prefect_gcp()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Goodbye, prefect-gcp!"
