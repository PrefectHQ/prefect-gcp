"""This is an example flows module"""
from prefect import flow

from prefect_gcp.tasks import (
    goodbye_prefect_gcp,
    hello_prefect_gcp,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_gcp)
    print(goodbye_prefect_gcp)
