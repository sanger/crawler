import pandas as pd
import numpy as np
from crawler.filtered_positive_identifier import (
    FilteredPositiveIdentifier,
    FILTERED_POSITIVE_VERSION_0,
    FILTERED_POSITIVE_VERSION_1,
    FILTERED_POSITIVE_VERSION_2,
)
from migrations.helpers.update_legacy_filtered_positives_helper import (
    check_versions_set,
    unmigrated_mongo_samples,
    get_cherrypicked_samples_by_date,
    split_mongo_samples_by_version,
)

from crawler.constants import (
    FIELD_COORDINATE,
    FIELD_PLATE_BARCODE,
    FIELD_ROOT_SAMPLE_ID,
    V0_V1_CUTOFF_TIMESTAMP,
    V1_V2_CUTOFF_TIMESTAMP,
)

# ----- migration helper function tests -----


def test_unmigrated_mongo_samples_returns_expected(config, filtered_positive_testing_samples):
    result = unmigrated_mongo_samples(config)
    assert len(result) == 2


def test_check_versions_set_returns_true_with_v0_v1_samples(config, filtered_positive_testing_samples_v0_v1):
    assert check_versions_set(config) is True


def test_check_versions_set_returns_false_with_no_v0_v1_samples(config, filtered_positive_testing_samples):
    assert check_versions_set(config) is False


def test_get_cherrypicked_samples_by_date_v0_returns_expected(
    config, event_wh_data, mlwh_sample_stock_resource, mlwh_sql_engine, event_wh_sql_engine
):
    root_sample_ids = ["root_1", "root_2", "root_3", "root_4"]
    plate_barcodes = ["pb_1", "pb_2", "pb_3", "pb_4"]

    expected_rows = [["root_1", "pb_1"], ["root_2", "pb_2"]]
    expected_columns = [FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE]
    expected = pd.DataFrame(np.array(expected_rows), columns=expected_columns, index=[0, 1])

    returned_samples = get_cherrypicked_samples_by_date(
        config, root_sample_ids, plate_barcodes, '1970-01-01 00:00:01', V0_V1_CUTOFF_TIMESTAMP
    )
    pd.testing.assert_frame_equal(expected, returned_samples)


def test_get_cherrypicked_samples_by_date_v1_returns_expected(
    config, event_wh_data, mlwh_sample_stock_resource, mlwh_sql_engine, event_wh_sql_engine
):
    root_sample_ids = ["root_1", "root_2", "root_3", "root_4"]
    plate_barcodes = ["pb_1", "pb_2", "pb_3", "pb_4"]

    expected_rows = [["root_1", "pb_1"], ["root_3", "pb_3"]]
    expected_columns = [FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE]
    expected = pd.DataFrame(np.array(expected_rows), columns=expected_columns, index=[0, 1])

    returned_samples = get_cherrypicked_samples_by_date(
        config, root_sample_ids, plate_barcodes, V0_V1_CUTOFF_TIMESTAMP, V1_V2_CUTOFF_TIMESTAMP
    )
    pd.testing.assert_frame_equal(expected, returned_samples)


def test_split_mongo_samples_by_version(unmigrated_mongo_testing_samples):
    rows = [["MCM005", "456"], ["MCM006", "456"]]
    columns = [FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE]
    v0_cherrypicked_samples = pd.DataFrame(np.array(rows), columns=columns, index=[0, 1])

    rows = [["MCM007", "456"]]
    columns = [FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE]
    v1_cherrypicked_samples = pd.DataFrame(np.array(rows), columns=columns, index=[0])

    v0_unmigrated_samples = unmigrated_mongo_testing_samples[1:3]
    v1_unmigrated_samples = unmigrated_mongo_testing_samples[-1:]
    v2_unmigrated_samples = unmigrated_mongo_testing_samples[:1]
    
    expected = {
        FILTERED_POSITIVE_VERSION_0: v0_unmigrated_samples,
        FILTERED_POSITIVE_VERSION_1: v1_unmigrated_samples,
        FILTERED_POSITIVE_VERSION_2: v2_unmigrated_samples,
    }

    samples_by_version = split_mongo_samples_by_version(
        unmigrated_mongo_testing_samples, v0_cherrypicked_samples, v1_cherrypicked_samples
    )

    assert samples_by_version == expected
