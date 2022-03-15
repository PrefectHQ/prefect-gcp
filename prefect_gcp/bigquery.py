"""Tasks for interacting with GCP BigQuery"""

from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

from anyio import to_thread
from google.cloud.bigquery import (
    LoadJobConfig,
    QueryJobConfig,
    ScalarQueryParameter,
    SchemaField,
    Table,
    TimePartitioning,
)
from google.cloud.exceptions import NotFound
from prefect import get_run_logger, task

if TYPE_CHECKING:
    from google.cloud.bigquery.table import Row

    from .credentials import GcpCredentials


def _query_sync(client, query, to_dataframe, **kwargs):
    """
    Helper function to ensure query + result are run on a single thread.
    """
    result = client.query(query, **kwargs).result()
    if to_dataframe:
        return result.to_dataframe()
    else:
        return list(result)


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
            [Google documentation](https://cloud.google.com/bigquery/docs/parameterized-queries#bigquery-query-params-python)
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
    """  # noqa
    logger = get_run_logger()
    logger.info("Running BigQuery query")

    client = gcp_credentials.get_bigquery_client(project=project)

    # setup job config
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
        _query_sync,
        client,
        query,
        to_dataframe,
        location=location,
        job_config=job_config,
    )
    result = await to_thread.run_sync(partial_query)
    return result


@task
async def bigquery_create_table(
    dataset: str,
    table: str,
    schema: List[SchemaField],
    gcp_credentials: "GcpCredentials",
    clustering_fields: List[str] = None,
    time_partitioning: TimePartitioning = None,
    project: Optional[str] = None,
    location: str = "US",
):
    """
    Creates table in BigQuery.

    Args:
        dataset: Name of a dataset in that the table will be created.
        table: Name of a table to create.
        schema: Schema to use when creating the table.
        gcp_credentials: Credentials to use for authentication with GCP.
        clustering_fields: List of fields to cluster the table by.
        time_partitioning: `bigquery.TimePartitioning` object specifying a partitioning
            of the newly created table
        project: Project to initialize the BigQuery Client with; if
            not provided, will default to the one inferred from your credentials.
        location: location of the dataset that will be written to.

    Returns:
        Table name.

    Raises:
        SUCCESS: a `SUCCESS` signal if the table already exists
    """
    logger = get_run_logger()
    logger.info("Creating %s.%s", dataset, table)

    client = gcp_credentials.get_bigquery_client(project=project)
    try:
        dataset_ref = client.get_dataset(dataset)
    except NotFound:
        logger.debug("Dataset %s not found, creating", dataset)
        dataset_ref = client.create_dataset(dataset)

    table_ref = dataset_ref.table(table)
    try:
        client.get_table(table_ref)
        logger.info("%s.%s already exists", dataset, table)
    except NotFound:
        logger.debug("Table %s not found, creating...", table)
        table = Table(table_ref, schema=schema)

        # partitioning
        if time_partitioning:
            table.time_partitioning = time_partitioning

        # cluster for optimal data sorting/access
        if clustering_fields:
            table.clustering_fields = clustering_fields

        client.create_table(table)

    return table


@task
async def bigquery_streaming_insert(
    records: List[dict],
    dataset_id: str,
    table: str,
    gcp_credentials: "GcpCredentials",
    project: Optional[str] = None,
    location: str = "US",
):
    """
    Insert records in a Google BigQuery table via the [streaming
    API](https://cloud.google.com/bigquery/streaming-data-into-bigquery).

    Args:
        records: The list of records to insert as rows into the BigQuery table;
            each item in the list should be a dictionary whose keys correspond to
            columns in the table.
        dataset_id: The id of a destination dataset to write the records to;
            if not provided here, will default to the one provided at initialization.
        table: The name of a destination table to write the records to;
            if not provided here, will default to the one provided at initialization.
        gcp_credentials: Credentials to use for authentication with GCP.
        project: The project to initialize the BigQuery Client with; if
            not provided, will default to the one inferred from your credentials.
        location: Location of the dataset that will be written to.

    Raises:
        ValueError: if any of the records result in errors.

    Returns:
        The response from `insert_rows_json`.

    Example:

    """
    client = gcp_credentials.get_bigquery_client(project=project, location=location)
    table_ref = client.dataset(dataset_id).table(table)
    partial_insert = partial(
        client.insert_rows_json, table=table_ref, json_rows=records
    )
    response = await to_thread.run_sync(partial_insert)

    errors = []
    output = []
    for row in response:
        output.append(row)
        if "errors" in row:
            errors.append(row["errors"])

    if errors:
        raise ValueError(errors)

    return output


@task
def bigquery_load_cloud_storage(
    uri: str,
    dataset_id: str,
    table: str,
    schema: List[SchemaField],
    gcp_credentials: "GcpCredentials",
    job_config: Optional[dict] = None,
    project: Optional[str] = None,
    location: str = "US",
):
    """
    Run method for this Task.  Invoked by _calling_ this
    Task within a Flow context, after initialization.
    Args:
        uri: GCS path to load data from.
        dataset_id: The id of a destination dataset to write the records to.
        table: The name of a destination table to write the records to.
        schema: The schema to use when creating the table.
        gcp_credentials: Credentials to use for authentication with GCP.
        job_config: an optional dictionary of job configuration parameters;
            note that the parameters provided here must be pickleable
            (e.g., dataset references will be rejected).
        project: The project to initialize the BigQuery Client with; if
            not provided, will default to the one inferred from your credentials.
        location: Location of the dataset that will be written to.

    Raises:
        ValueError: if all required arguments haven't been provided.
        ValueError: if the load job results in an error.

    Returns:
        The response from `load_table_from_uri`.
    """
    logger = get_run_logger()

    # create client
    client = gcp_credentials.get_bigquery_client(project=project)

    # get table reference
    table_ref = client.dataset(dataset_id).table(table)

    # load data
    job_config = job_config or {}
    if "autodetect" not in job_config:
        job_config["autodetect"] = True

    job_config = LoadJobConfig(**job_config)
    if schema:
        job_config.schema = schema

    load_job = None
    try:
        load_job = client.load_table_from_uri(
            uri,
            table_ref,
            location=location,
            job_config=job_config,
        )
        load_job.result()
        # remove unpickleable attributes
        load_job._client = None
        load_job._completion_lock = None

    except Exception as exception:
        logger.exception(exception)
        if load_job is not None and load_job.errors is not None:
            for error in load_job.errors:
                logger.exception(error)
        raise

    return load_job


@task
def bigquery_load_file(
    file: Union[str, Path],
    dataset_id: str,
    table: str,
    schema: List[SchemaField],
    gcp_credentials: "GcpCredentials",
    job_config: Optional[dict] = None,
    rewind: bool = False,
    size: int = None,
    num_retries: int = 6,
    project: Optional[str] = None,
    location: str = "US",
):
    """
    Loads file into BigQuery.

    Args:
        file: A string or path-like object of the file to be loaded.
        dataset_id: ID of a destination dataset to write the records to;
            if not provided here, will default to the one provided at initialization.
        table: Name of a destination table to write the records to;
            if not provided here, will default to the one provided at initialization.
        schema: Schema to use when creating the table.
        gcp_credentials: Credentials to use for authentication with GCP.
        job_config: An optional dictionary of job configuration parameters;
            note that the parameters provided here must be pickleable
            (e.g., dataset references will be rejected).
        rewind: if True, seek to the beginning of the file handle
            before reading the file.
        size: Number of bytes to read from the file handle. If size is None or large,
            resumable upload will be used. Otherwise, multipart upload will be used.
        num_retries: the number of max retries for loading the bigquery table from file.
        project: Project to initialize the BigQuery Client with; if
            not provided, will default to the one inferred from your credentials.
        location: location of the dataset that will be written to.

    Raises:
        ValueError: if all required arguments haven't been provided
            or file does not exist
        IOError: if file can't be opened and read
        ValueError: if the load job results in an error

    Returns:
        The response from `load_table_from_file`
    """
    logger = get_run_logger()

    # check for any argument inconsistencies
    if dataset_id is None or table is None:
        raise ValueError("Both dataset_id and table must be provided.")
    try:
        path = Path(file)
    except Exception as value_error:
        raise ValueError(
            "A string or path-like object must be provided."
        ) from value_error
    if not path.is_file():
        raise ValueError(f"File {path.as_posix()} does not exist.")

    # create client
    client = gcp_credentials.get_bigquery_client(project=project)

    # get table reference
    table_ref = client.dataset(dataset_id).table(table)

    # configure job
    job_config = job_config or {}
    if "autodetect" not in job_config:
        job_config["autodetect"] = True
    job_config = LoadJobConfig(**job_config)
    if schema:
        job_config.schema = schema

    # load data
    try:
        with open(file, "rb") as file_obj:
            load_job = client.load_table_from_file(
                file_obj,
                table_ref,
                rewind,
                size,
                num_retries,
                location=location,
                job_config=job_config,
            )
            # remove unpickleable attributes
            load_job._client = None
            load_job._completion_lock = None
    except IOError:
        logger.exception(f"Can't open and read from {path.as_posix()}.")
        raise

    load_job.result()  # block until job is finished

    return load_job
