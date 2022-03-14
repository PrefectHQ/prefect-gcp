"""Tasks for interacting with GCP BigQuery"""

from functools import partial
from typing import TYPE_CHECKING, List, Optional

from anyio import to_thread
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from prefect import get_run_logger, task

if TYPE_CHECKING:
    from google.cloud.bigquery.table import Row

    from .credentials import GcpCredentials


@task
async def bigquery_query(
    query: str,
    gcp_credentials: "GcpCredentials",
    query_params: Optional[List[tuple]] = None,  # 3-tuples
    dry_run_max_bytes: Optional[int] = None,
    dataset_dest: Optional[str] = None,
    table_dest: Optional[str] = None,
    to_dataframe: bool = False,
    job_config: Optional[dict] = None,
    project: Optional[str] = None,
    location: str = "US",
) -> List["Row"]:
    """
    Runs a BigQuery query.

    Args:
        query: String of the query to execute.
        gcp_credentials: Credentials to use for authentication with GCP.
        query_params: List of 3-tuples specifying BigQuery query parameters; currently
            only scalar query parameters are supported.  See the
            [Google documentation](https://cloud.google.com/bigquery/docs/parameterized-queries#bigquery-query-params-python)  # noqa
            for more details on how both the query and the query parameters should be formatted.
        dry_run_max_bytes: If provided, the maximum number of bytes the query
            is allowed to process; this will be determined by executing a dry run
            and raising a `ValueError` if the maximum is exceeded.
        dataset_dest: the optional name of a destination dataset to write the
            query results to, if you don't want them returned; if provided,
            `table_dest` must also be provided.
        table_dest: the optional name of a destination table to write the
            query results to, if you don't want them returned; if provided,
            `dataset_dest` must also be provided.
        to_dataframe: if provided, returns the results of the query as a pandas
            dataframe instead of a list of `bigquery.table.Row` objects.
        job_config: an optional dictionary of job configuration parameters;
            note that the parameters provided here must be pickleable
            (e.g., dataset references will be rejected).
        project: The project to initialize the BigQuery Client with; if not
            provided, will default to the one inferred from your credentials.
        location: Location of the dataset that will be queried.

    Returns:
        A list of rows, or pandas DataFrame if to_dataframe,
        matching the query criteria.

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
            query = '''
                SELECT word, word_count
                FROM `bigquery-public-data.samples.shakespeare`
                WHERE corpus = @corpus
                AND word_count >= @min_word_count
                ORDER BY word_count DESC;
            '''
            query_params = [
                ("corpus", "STRING", "romeoandjuliet"),
                ("min_word_count", "INT64", 250)
            ]
            result = bigquery_query(
                query, gcp_credentials, query_params=query_params
            )
            return result

        example_bigquery_query_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Running query")

    client = gcp_credentials.get_bigquery_client(project=project)

    # setup jobconfig
    job_config = QueryJobConfig(**job_config or {})
    if query_params is not None:
        job_config.query_parameters = [ScalarQueryParameter(*qp) for qp in query_params]

    # perform dry_run if requested
    if dry_run_max_bytes is not None:
        saved_info = dict(
            dry_run=job_config.dry_run, use_query_cache=job_config.use_query_cache
        )
        job_config.dry_run = True
        job_config.use_query_cache = False
        partial_query = partial(
            client.query, query, location=location, job_config=job_config
        )
        response = await to_thread.run_sync(partial_query)
        total_bytes_processed = response.total_bytes_processed
        if total_bytes_processed > dry_run_max_bytes:
            raise RuntimeError(
                f"Query will process {total_bytes_processed} bytes which is above "
                f"the set maximum of {dry_run_max_bytes} for this task."
            )
        job_config.dry_run = saved_info["dry_run"]
        job_config.use_query_cache = saved_info["use_query_cache"]

    # if writing to a destination table
    if dataset_dest is not None:
        table_ref = client.dataset(dataset_dest).table(table_dest)
        job_config.destination = table_ref

    partial_query = partial(
        client.query, query, location=location, job_config=job_config
    )
    response = await to_thread.run_sync(partial_query)

    partial_result = partial(response.result)
    result = await to_thread.run_sync(partial_result)
    if to_dataframe:
        return result.to_dataframe()
    else:
        return list(result)
