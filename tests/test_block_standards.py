import pytest
from prefect.testing.standard_test_suites import BlockStandardTestSuite

from prefect_gcp.cloud_storage import GcsBucket


@pytest.mark.parametrize("block", [GcsBucket])
class TestAllBlocksAdhereToStandards(BlockStandardTestSuite):
    @pytest.fixture
    def block(self, block):
        return block
