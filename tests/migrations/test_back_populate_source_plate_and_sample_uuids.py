from datetime import datetime
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from crawler.helpers.general_helpers import get_config
from migrations import back_populate_source_plate_and_sample_uuids

# ----- test fixture helpers -----


@pytest.fixture
def mock_helper_imports():
    with patch("migrations.update_filtered_positives.pending_plate_barcodes_from_dart") as mock_get_plate_barcodes:
        with patch(
            "migrations.update_filtered_positives.positive_result_samples_from_mongo"
        ) as mock_get_positive_samples:
            yield mock_get_plate_barcodes, mock_get_positive_samples

# TODO needs a test barcodes file

# TODO need test samples mongo collection containing samples for plates in barcodes list in test file

# TODO need MLWH lighthouse_samples table containing samples for plates in barcodes list in test file

def test_back_populate_source_plate_uuid_and_sample_uuid_missing_file():
    filepath='not_found'
    back_populate_source_plate_and_sample_uuids.run('crawler.config.integration', filepath)

    # should throw an exception

    # ensure that no databases are updated
    # mock_update_uuids_mongo_and_mlwh.assert_not_called()
    # mock_update_mongo_sample_uuid_and_source_plate_uuid.assert_not_called()

def test_back_populate_source_plate_uuid_and_sample_uuid():
    filepath = './test/data/populate_old_plates.csv'
    back_populate_source_plate_and_sample_uuids.run('crawler.config.integration', filepath)