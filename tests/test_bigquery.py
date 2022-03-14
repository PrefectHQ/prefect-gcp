import pytest
from prefect import flow

from prefect_gcp.bigquery import bigquery_query


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
            dataset_dest="dataset_dest",
            table_dest="table_dest",
            job_config={},
            project="project",
            location="US",
        )

    if dry_run_max_bytes is not None and dry_run_max_bytes < 10:
        with pytest.raises(RuntimeError):
            test_flow().result().result()
    else:
        result = test_flow().result().result()
        if to_dataframe:
            assert result == "dataframe_query"
        else:
            assert result == ["query"]
