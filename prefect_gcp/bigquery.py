"""Tasks for interacting with GCP BigQuery"""

import os
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union

from anyio import to_thread
from google.cloud.bigquery import (
    ExternalConfig,
    LoadJob,
    LoadJobConfig,
    QueryJobConfig,
    ScalarQueryParameter,
    SchemaField,
    Table,
    TimePartitioning,
)
from google.cloud.bigquery.dbapi.connection import Connection
from google.cloud.bigquery.dbapi.cursor import Cursor
from google.cloud.bigquery.table import Row
from google.cloud.exceptions import NotFound
from prefect import get_run_logger, task
from prefect.blocks.abstract import DatabaseBlock
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import Field

from prefect_gcp.credentials import GcpCredentials


def _result_sync(func, *args, **kwargs):
    """
    Helper function to ensure result is run on a single thread.
    """
    result = func(*args, **kwargs).result()
    return result


@task
async def bigquery_query(
    query: str,
    gcp_credentials: "GcpCredentials",
    query_params: Optional[List[tuple]] = None,  # 3-tuples
    dry_run_max_bytes: Optional[int] = None,
    dataset: Optional[str] = None,
    table: Optional[str] = None,
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
        dataset: Name of a destination dataset to write the query results to,
            if you don't want them returned; if provided, `table` must also be provided.
        table: Name of a destination table to write the query results to,
            if you don't want them returned; if provided, `dataset` must also be provided.
        to_dataframe: If provided, returns the results of the query as a pandas
            dataframe instead of a list of `bigquery.table.Row` objects.
        job_config: Dictionary of job configuration parameters;
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

        @flow
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

    client = gcp_credentials.get_bigquery_client(project=project, location=location)

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
        partial_query = partial(client.query, query, job_config=job_config)
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
    if dataset is not None:
        table_ref = client.dataset(dataset).table(table)
        job_config.destination = table_ref

    partial_query = partial(
        _result_sync,
        client.query,
        query,
        job_config=job_config,
    )
    result = await to_thread.run_sync(partial_query)
    if to_dataframe:
        return result.to_dataframe()
    else:
        return list(result)


@task
async def bigquery_create_table(
    dataset: str,
    table: str,
    gcp_credentials: "GcpCredentials",
    schema: Optional[List[SchemaField]] = None,
    clustering_fields: List[str] = None,
    time_partitioning: TimePartitioning = None,
    project: Optional[str] = None,
    location: str = "US",
    external_config: Optional[ExternalConfig] = None,
) -> str:
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
        location: The location of the dataset that will be written to.
        external_config: The [external data source](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_table#nested_external_data_configuration).  # noqa
    Returns:
        Table name.
    Example:
        ```python
        from prefect import flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.bigquery import bigquery_create_table
        from google.cloud.bigquery import SchemaField
        @flow
        def example_bigquery_create_table_flow():
            gcp_credentials = GcpCredentials(project="project")
            schema = [
                SchemaField("number", field_type="INTEGER", mode="REQUIRED"),
                SchemaField("text", field_type="STRING", mode="REQUIRED"),
                SchemaField("bool", field_type="BOOLEAN")
            ]
            result = bigquery_create_table(
                dataset="dataset",
                table="test_table",
                schema=schema,
                gcp_credentials=gcp_credentials
            )
            return result
        example_bigquery_create_table_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Creating %s.%s", dataset, table)

    if not external_config and not schema:
        raise ValueError("Either a schema or an external config must be provided.")

    client = gcp_credentials.get_bigquery_client(project=project, location=location)
    try:
        partial_get_dataset = partial(client.get_dataset, dataset)
        dataset_ref = await to_thread.run_sync(partial_get_dataset)
    except NotFound:
        logger.debug("Dataset %s not found, creating", dataset)
        partial_create_dataset = partial(client.create_dataset, dataset)
        dataset_ref = await to_thread.run_sync(partial_create_dataset)

    table_ref = dataset_ref.table(table)
    try:
        partial_get_table = partial(client.get_table, table_ref)
        await to_thread.run_sync(partial_get_table)
        logger.info("%s.%s already exists", dataset, table)
    except NotFound:
        logger.debug("Table %s not found, creating", table)
        table_obj = Table(table_ref, schema=schema)

        # external data configuration
        if external_config:
            table_obj.external_data_configuration = external_config

        # cluster for optimal data sorting/access
        if clustering_fields:
            table_obj.clustering_fields = clustering_fields

        # partitioning
        if time_partitioning:
            table_obj.time_partitioning = time_partitioning

        partial_create_table = partial(client.create_table, table_obj)
        await to_thread.run_sync(partial_create_table)

    return table


@task
async def bigquery_insert_stream(
    dataset: str,
    table: str,
    records: List[dict],
    gcp_credentials: "GcpCredentials",
    project: Optional[str] = None,
    location: str = "US",
) -> List:
    """
    Insert records in a Google BigQuery table via the [streaming
    API](https://cloud.google.com/bigquery/streaming-data-into-bigquery).

    Args:
        dataset: Name of a dataset where the records will be written to.
        table: Name of a table to write to.
        records: The list of records to insert as rows into the BigQuery table;
            each item in the list should be a dictionary whose keys correspond to
            columns in the table.
        gcp_credentials: Credentials to use for authentication with GCP.
        project: The project to initialize the BigQuery Client with; if
            not provided, will default to the one inferred from your credentials.
        location: Location of the dataset that will be written to.

    Returns:
        List of inserted rows.

    Example:
        ```python
        from prefect import flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.bigquery import bigquery_insert_stream
        from google.cloud.bigquery import SchemaField

        @flow
        def example_bigquery_insert_stream_flow():
            gcp_credentials = GcpCredentials(project="project")
            records = [
                {"number": 1, "text": "abc", "bool": True},
                {"number": 2, "text": "def", "bool": False},
            ]
            result = bigquery_insert_stream(
                dataset="integrations",
                table="test_table",
                records=records,
                gcp_credentials=gcp_credentials
            )
            return result

        example_bigquery_insert_stream_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Inserting into %s.%s as a stream", dataset, table)

    client = gcp_credentials.get_bigquery_client(project=project, location=location)
    table_ref = client.dataset(dataset).table(table)
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
async def bigquery_load_cloud_storage(
    dataset: str,
    table: str,
    uri: str,
    gcp_credentials: "GcpCredentials",
    schema: Optional[List[SchemaField]] = None,
    job_config: Optional[dict] = None,
    project: Optional[str] = None,
    location: str = "US",
) -> LoadJob:
    """
    Run method for this Task.  Invoked by _calling_ this
    Task within a Flow context, after initialization.
    Args:
        uri: GCS path to load data from.
        dataset: The id of a destination dataset to write the records to.
        table: The name of a destination table to write the records to.
        gcp_credentials: Credentials to use for authentication with GCP.
        schema: The schema to use when creating the table.
        job_config: Dictionary of job configuration parameters;
            note that the parameters provided here must be pickleable
            (e.g., dataset references will be rejected).
        project: The project to initialize the BigQuery Client with; if
            not provided, will default to the one inferred from your credentials.
        location: Location of the dataset that will be written to.

    Returns:
        The response from `load_table_from_uri`.

    Example:
        ```python
        from prefect import flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.bigquery import bigquery_load_cloud_storage

        @flow
        def example_bigquery_load_cloud_storage_flow():
            gcp_credentials = GcpCredentials(project="project")
            result = bigquery_load_cloud_storage(
                dataset="dataset",
                table="test_table",
                uri="uri",
                gcp_credentials=gcp_credentials
            )
            return result

        example_bigquery_load_cloud_storage_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Loading into %s.%s from cloud storage", dataset, table)

    client = gcp_credentials.get_bigquery_client(project=project, location=location)
    table_ref = client.dataset(dataset).table(table)

    job_config = job_config or {}
    if "autodetect" not in job_config:
        job_config["autodetect"] = True
    job_config = LoadJobConfig(**job_config)
    if schema:
        job_config.schema = schema

    result = None
    try:
        partial_load = partial(
            _result_sync,
            client.load_table_from_uri,
            uri,
            table_ref,
            job_config=job_config,
        )
        result = await to_thread.run_sync(partial_load)
    except Exception as exception:
        logger.exception(exception)
        if result is not None and result.errors is not None:
            for error in result.errors:
                logger.exception(error)
        raise

    if result is not None:
        # remove unpickleable attributes
        result._client = None
        result._completion_lock = None

    return result


@task
async def bigquery_load_file(
    dataset: str,
    table: str,
    path: Union[str, Path],
    gcp_credentials: "GcpCredentials",
    schema: Optional[List[SchemaField]] = None,
    job_config: Optional[dict] = None,
    rewind: bool = False,
    size: Optional[int] = None,
    project: Optional[str] = None,
    location: str = "US",
) -> LoadJob:
    """
    Loads file into BigQuery.

    Args:
        dataset: ID of a destination dataset to write the records to;
            if not provided here, will default to the one provided at initialization.
        table: Name of a destination table to write the records to;
            if not provided here, will default to the one provided at initialization.
        path: A string or path-like object of the file to be loaded.
        gcp_credentials: Credentials to use for authentication with GCP.
        schema: Schema to use when creating the table.
        job_config: An optional dictionary of job configuration parameters;
            note that the parameters provided here must be pickleable
            (e.g., dataset references will be rejected).
        rewind: if True, seek to the beginning of the file handle
            before reading the file.
        size: Number of bytes to read from the file handle. If size is None or large,
            resumable upload will be used. Otherwise, multipart upload will be used.
        project: Project to initialize the BigQuery Client with; if
            not provided, will default to the one inferred from your credentials.
        location: location of the dataset that will be written to.

    Returns:
        The response from `load_table_from_file`.

    Example:
        ```python
        from prefect import flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.bigquery import bigquery_load_file
        from google.cloud.bigquery import SchemaField

        @flow
        def example_bigquery_load_file_flow():
            gcp_credentials = GcpCredentials(project="project")
            result = bigquery_load_file(
                dataset="dataset",
                table="test_table",
                path="path",
                gcp_credentials=gcp_credentials
            )
            return result

        example_bigquery_load_file_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Loading into %s.%s from file", dataset, table)

    if not os.path.exists(path):
        raise ValueError(f"{path} does not exist")
    elif not os.path.isfile(path):
        raise ValueError(f"{path} is not a file")

    client = gcp_credentials.get_bigquery_client(project=project)
    table_ref = client.dataset(dataset).table(table)

    job_config = job_config or {}
    if "autodetect" not in job_config:
        job_config["autodetect"] = True
        # TODO: test if autodetect is needed when schema is passed
    job_config = LoadJobConfig(**job_config)
    if schema:
        # TODO: test if schema can be passed directly in job_config
        job_config.schema = schema

    try:
        with open(path, "rb") as file_obj:
            partial_load = partial(
                _result_sync,
                client.load_table_from_file,
                file_obj,
                table_ref,
                rewind=rewind,
                size=size,
                location=location,
                job_config=job_config,
            )
            result = await to_thread.run_sync(partial_load)
    except IOError:
        logger.exception(f"Could not open and read from {path}")
        raise

    if result is not None:
        # remove unpickleable attributes
        result._client = None
        result._completion_lock = None

    return result


class BigQuery(DatabaseBlock):
    """
    A block for querying a database with BigQuery.

    Attributes:
        gcp_credentials: The credentials to use to authenticate.
        fetch_size: The number of rows to fetch at a time when calling fetch_many.
            Note, this parameter is executed on the client side and is not
            passed to the database. To limit on the server side, add the `LIMIT`
            clause, or the dialect's equivalent clause, like `TOP`, to the query.
    """

    gcp_credentials: GcpCredentials
    fetch_size: int = Field(
        default=1, description="The number of rows to fetch at a time."
    )

    _connection: Optional[Connection] = None

    @contextmanager
    def manage_connection(self) -> Generator[Connection, None, None]:
        """
        Get a BigQuery connection and close upon completion.

        Yields:
            A BigQuery connection.
        """
        if self._connection:
            yield self._connection
        else:
            with self.gcp_credentials.get_bigquery_client() as client:
                connection = Connection(client=client)
                yield connection
                connection.close()

    @sync_compatible
    async def fetch_one(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        cursor: Optional[Cursor] = None,
        **execution_options: Dict[str, Any],
    ) -> Row:
        """
        Fetch a single result from the database.

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            cursor: A cursor to use for the operation; if not provided, will
                automatically create one.
            **execution_options: Additional options to pass to `connection.execute`.

        Returns:
            A tuple containing the data returned by the database,
                where each row is a tuple and each column is a value in the tuple.
        """
        with self.manage_connection() as connection:
            cursor = cursor or connection.cursor()
            if cursor._query_data is None:
                await run_sync_in_worker_thread(
                    cursor.execute,
                    operation,
                    parameters=parameters,
                    **execution_options,
                )
            result = cursor.fetchone()

        return result

    @sync_compatible
    async def fetch_many(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        size: Optional[int] = None,
        cursor: Optional[Cursor] = None,
        **execution_options: Dict[str, Any],
    ) -> List[Row]:
        """
        Fetch a limited number of results from the database.

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            size: The number of results to return; if None or 0, uses the value of
                `fetch_size` configured on the block.
            cursor: A cursor to use for the operation; if not provided, will
                automatically create one.
            **execution_options: Additional options to pass to `connection.execute`.

        Returns:
            A list of tuples containing the data returned by the database,
                where each row is a tuple and each column is a value in the tuple.
        """
        with self.manage_connection() as connection:
            cursor = cursor or connection.cursor()
            if cursor._query_data is None:
                await run_sync_in_worker_thread(
                    cursor.execute,
                    operation,
                    parameters=parameters,
                    **execution_options,
                )
            size = size or self.fetch_size
            result = cursor.fetchmany(size)
        return result

    @sync_compatible
    async def fetch_all(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        cursor: Optional[Cursor] = None,
        **execution_options: Dict[str, Any],
    ) -> List[Row]:
        """
        Fetch all results from the database.

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            cursor: A cursor to use for the operation; if not provided, will
                automatically create one.
            **execution_options: Additional options to pass to `connection.execute`.

        Returns:
            A list of tuples containing the data returned by the database,
                where each row is a tuple and each column is a value in the tuple.
        """
        with self.manage_connection() as connection:
            cursor = cursor or connection.cursor()
            await run_sync_in_worker_thread(
                cursor.execute, operation, parameters=parameters, **execution_options
            )
            result = cursor.fetchall()
        return result

    @sync_compatible
    async def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        **execution_options: Dict[str, Any],
    ) -> None:
        """
        Executes an operation on the database. This method is intended to be used
        for operations that do not return data, such as INSERT, UPDATE, or DELETE.

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            **execution_options: Additional options to pass to `connection.execute`.
        """
        with self.manage_connection() as connection:
            cursor = connection.cursor()
            await run_sync_in_worker_thread(
                cursor.execute, operation, parameters=parameters, **execution_options
            )

    @sync_compatible
    async def execute_many(
        self,
        operation: str,
        seq_of_parameters: List[Dict[str, Any]],
    ) -> None:
        """
        Executes many operations on the database. This method is intended to be used
        for operations that do not return data, such as INSERT, UPDATE, or DELETE.

        Args:
            operation: The SQL query or other operation to be executed.
            seq_of_parameters: The sequence of parameters for the operation.
        """
        with self.manage_connection() as connection:
            cursor = connection.cursor()  # this gets closed by connection
            await run_sync_in_worker_thread(
                cursor.executemany, operation, seq_of_parameters=seq_of_parameters
            )

    def __enter__(self):
        """
        Start a connection upon entry.
        """
        with self.gcp_credentials.get_bigquery_client() as client:
            self._connection = Connection(client=client)
        return self

    def __exit__(self, *args):
        self._connection.close()
        self._connection = None
