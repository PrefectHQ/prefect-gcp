import pytest
from prefect._internal.compatibility.deprecated import PrefectDeprecationWarning

from prefect_gcp.aiplatform import VertexAICustomTrainingJob
from prefect_gcp.cloud_run import CloudRunJob


@pytest.mark.parametrize(
    "InfraBlock, expected_message",
    [
        (
            CloudRunJob,
            "prefect_gcp.cloud_run.CloudRunJob has been deprecated."
            " It will not be available after Sep 2024."
            " Use the Vertex AI worker instead."
            " Refer to the upgrade guide for more information",
        ),
        (
            VertexAICustomTrainingJob,
            "prefect_gcp.aiplaform.VertexAICustomTrainingJob has been deprecated."
            " It will not be available after Sep 2024."
            " Use the Cloud Run or Cloud Run v2 worker instead."
            " Refer to the upgrade guide for more information",
        ),
    ],
)
def test_infra_blocks_emit_a_deprecation_warning(InfraBlock, expected_message):
    with pytest.warns(PrefectDeprecationWarning, match=expected_message):
        InfraBlock()
