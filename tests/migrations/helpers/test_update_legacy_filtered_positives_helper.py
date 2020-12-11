import pytest
import pandas as pd
import numpy as np

from migrations.helpers.update_legacy_filtered_positives_helper import (
    v0_version_set,
    unmigrated_mongo_samples,
    get_v0_cherrypicked_samples,
)

from crawler.constants import (
    FIELD_COORDINATE,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SOURCE,
)

# ----- migration helper function tests -----

def test_unmigrated_mongo_samples_returns_expected(config, filtered_positive_testing_samples):
    result = unmigrated_mongo_samples(config)
    assert len(result) == 1


def test_v0_version_set_returns_true_with_v0_samples(config, filtered_positive_testing_samples):
    assert v0_version_set(config) == True


def test_v0_version_set_returns_false_with_no_v0_samples(config, v1_filtered_positive_testing_samples):
    assert v0_version_set(config) == False


def test_v0_version_set_returns_false_with_no_version_fields(config, testing_samples):
    assert v0_version_set(config) == False


def test_get_v0_cherrypicked_samples_returns_expected(
    config, event_wh_data, mlwh_sample_stock_resource, mlwh_sql_engine, event_wh_sql_engine
):
    # root_4 does not exist in MLWH
    root_sample_ids = ["root_1", "root_2", "root_3", "root_4"]
    plate_barcodes = ["pb_1", "pb_2", "pb_3", "root_4"]

    expected_rows = [["root_1", "pb_1", "positive", "A1"], ["root_2", "pb_2", "positive", "A1"]]
    expected_columns = [FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE, "Result_lower", FIELD_COORDINATE]
    expected = pd.DataFrame(np.array(expected_rows), columns=expected_columns, index=[0, 1])

    returned_samples = get_v0_cherrypicked_samples(config, root_sample_ids, plate_barcodes)
    pd.testing.assert_frame_equal(expected, returned_samples)
