"""Tasks for interacting with GCP BigQuery"""

from functools import partial
from typing import TYPE_CHECKING, List

from anyio import to_thread
from bigquery import QueryJobConfig, ScalarQueryParameter
from prefect import get_run_logger, task

if TYPE_CHECKING:
    from google.cloud.bigquery.table import Row

    from .credentials import GcpCredentials


@task
async def bigquery_query(
    query: str,
    gcp_credentials: "GcpCredentials",
    query_params: List[tuple] = None,
    job_config: dict = None,
) -> List["Row"]:
    """
    Runs a BigQuery query.

    Args:
        query: SQL query.
        gcp_credentials: Credentials to use for authentication with GCP.
        query_params: List of 3-tuples specifying BigQuery query parameters;
            currently only scalar query parameters are supported. See the
            [Google documentation](https://cloud.google.com/bigquery/docs/parameterized-queries#bigquery-query-params-python)  # noqa
            for more details on how both the query and the query parameters should beformatted
        job_config: an optional dictionary of job configuration parameters; note
            that the parameters provided here must be pickleable
            (e.g., dataset references will be rejected).

    Returns:
        A list of rows matching the query criteria.

    Example:
        Queries the public names database, returning 10 results.
        ```python
        from prefect import flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.bigquery import bigquery_query

        @flow()
        def example_bigquery_query_flow():
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json",
                project="project"
            )
            query = (
                'SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` '
                'WHERE state = "TX" '
                'LIMIT 10')
            result = bigquery_query(query, gcp_credentials)
            return result

        example_bigquery_query_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Running query")

    client = gcp_credentials.get_bigquery_client()
    job_config = QueryJobConfig(**job_config)
    if query_params is not None:
        hydrated_params = [ScalarQueryParameter(*qp) for qp in query_params]
        job_config.query_parameters = hydrated_params
    partial_query = partial(client.query(query, job_config=job_config).result)
    result = await to_thread.run_sync(partial_query)
    return list(result)