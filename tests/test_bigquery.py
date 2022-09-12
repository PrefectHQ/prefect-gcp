import os

import pytest
from google.cloud.bigquery import SchemaField, ExternalConfig
from prefect import flow

from prefect_gcp.bigquery import (
    bigquery_create_table,
    bigquery_insert_stream,
    bigquery_load_cloud_storage,
    bigquery_load_file,
    bigquery_query,
)


@pytest.mark.parametrize("to_dataframe", [False, True])
@pytest.mark.parametrize("dry_run_max_bytes", [None, 5, 15])
def test_bigquery_query(to_dataframe, dry_run_max_bytes, gcp_credentials):
    @flow
    def test_flow():
        return bigquery_query(
            "query",
            gcp_credentials,
            to_dataframe=to_dataframe,
            query_params=[("param", str, "parameter")],
            dry_run_max_bytes=dry_run_max_bytes,
            dataset="dataset",
            table="test_table",
            job_config={},
            project="project",
            location="US",
        )

    if dry_run_max_bytes is not None and dry_run_max_bytes < 10:
        with pytest.raises(RuntimeError):
            test_flow()
    else:
        result = test_flow()
        if to_dataframe:
            assert result == "dataframe_query"
        else:
            assert result == ["query"]


def test_bigquery_create_table(gcp_credentials):
    @flow
    def test_flow():
        schema = [
            SchemaField("number", field_type="INTEGER", mode="REQUIRED"),
            SchemaField("text", field_type="STRING", mode="REQUIRED"),
            SchemaField("bool", field_type="BOOLEAN"),
        ]
        table = bigquery_create_table(
            "dataset",
            "table",
            schema,
            gcp_credentials,
            clustering_fields=["text"],
        )
        return table

    assert test_flow() == "table"


@pytest.mark.parametrize("external_config", [None, ExternalConfig(source_format="PARQUET")])
def test_bigquery_create_table_external(gcp_credentials, external_config):
    @flow
    def test_flow():
        table = bigquery_create_table(
            "dataset",
            "table",
            gcp_credentials,
            clustering_fields=["text"],
            external_config=external_config,
        )
        return table

    if external_config is None:
        with pytest.raises(ValueError):
            test_flow()
    else:
        assert test_flow() == "table"


def test_bigquery_insert_stream(gcp_credentials):

    records = [
        {"number": 1, "text": "abc", "bool": True},
        {"number": 2, "text": "def", "bool": False},
    ]

    @flow
    def test_flow():
        output = bigquery_insert_stream(
            "dataset",
            "table",
            records,
            gcp_credentials,
        )
        return output

    assert test_flow() == records


def test_bigquery_load_cloud_storage(gcp_credentials):
    @flow
    def test_flow():
        schema = [
            SchemaField("number", field_type="INTEGER", mode="REQUIRED"),
            SchemaField("text", field_type="STRING", mode="REQUIRED"),
            SchemaField("bool", field_type="BOOLEAN"),
        ]
        output = bigquery_load_cloud_storage(
            "dataset", "table", "uri", gcp_credentials, schema=schema
        )
        return output

    result = test_flow()
    assert result.output == "uri"
    assert result._client is None
    assert result._completion_lock is None


def test_bigquery_load_file(gcp_credentials):
    
    path = os.path.abspath(__file__)

    @flow
    def test_flow():
        schema = [
            SchemaField("number", field_type="INTEGER", mode="REQUIRED"),
            SchemaField("text", field_type="STRING", mode="REQUIRED"),
            SchemaField("bool", field_type="BOOLEAN"),
        ]
        output = bigquery_load_file(
            "dataset", "table", path, gcp_credentials, schema=schema
        )
        return output

    result = test_flow()
    assert result.output == "file"
    assert result._client is None
    assert result._completion_lock is None