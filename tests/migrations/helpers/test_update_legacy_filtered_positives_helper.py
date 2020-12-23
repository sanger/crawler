import pandas as pd
import numpy as np
from migrations.helpers.update_legacy_filtered_positives_helper import (
    v0_version_set,
    legacy_mongo_samples,
    get_cherrypicked_samples_by_date,
    split_mongo_samples_by_version,
)
from crawler.constants import (
    FIELD_PLATE_BARCODE,
    FIELD_ROOT_SAMPLE_ID,
    V0_V1_CUTOFF_TIMESTAMP,
    V1_V2_CUTOFF_TIMESTAMP,
)
from crawler.filtered_positive_identifier import (
    FILTERED_POSITIVE_VERSION_0,
    FILTERED_POSITIVE_VERSION_1,
    FILTERED_POSITIVE_VERSION_2,
)

# ----- migration helper function tests -----


def test_legacy_mongo_samples_returns_correct_samples_filtered_by_date(
    config, filtered_positive_testing_samples
):  # noqa: E501
    result = legacy_mongo_samples(config)
    expected_samples = filtered_positive_testing_samples[-3:]

    assert result == expected_samples


def test_check_versions_set_returns_true_with_v0(config, filtered_positive_testing_samples):
    assert v0_version_set(config) is True


def test_check_versions_set_returns_false_with_no_v0_samples(
    config, filtered_positive_testing_samples_no_v0
):  # noqa: E501
    assert v0_version_set(config) is False


def test_get_cherrypicked_samples_by_date_v0_returns_expected(
    config, event_wh_data, mlwh_sample_stock_resource
):  # noqa: E501
    root_sample_ids = ["root_1", "root_2", "root_3", "root_4"]
    plate_barcodes = ["pb_1", "pb_2", "pb_3", "pb_4"]

    expected_rows = [["root_2", "pb_2"]]
    expected_columns = [FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE]
    expected = pd.DataFrame(np.array(expected_rows), columns=expected_columns, index=[0])

    returned_samples = get_cherrypicked_samples_by_date(
        config, root_sample_ids, plate_barcodes, "1970-01-01 00:00:01", V0_V1_CUTOFF_TIMESTAMP
    )
    pd.testing.assert_frame_equal(expected, returned_samples)


def test_get_cherrypicked_samples_by_date_v1_returns_expected(
    config, event_wh_data, mlwh_sample_stock_resource
):  # noqa: E501
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

    samples_by_version = split_mongo_samples_by_version(
        unmigrated_mongo_testing_samples, v0_cherrypicked_samples, v1_cherrypicked_samples
    )

    for version, samples in samples_by_version.items():
        if version == FILTERED_POSITIVE_VERSION_0:
            assert samples == v0_unmigrated_samples
        elif version == FILTERED_POSITIVE_VERSION_1:
            assert samples == v1_unmigrated_samples
        elif version == FILTERED_POSITIVE_VERSION_2:
            assert samples == v2_unmigrated_samples
