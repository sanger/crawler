import pandas as pd
import pytest
import numpy as np
from datetime import datetime
from unittest.mock import patch
from migrations.helpers.update_legacy_filtered_positives_helper import (
    filtered_positive_fields_set,
    mongo_samples_by_date,
    get_cherrypicked_samples_by_date,
    split_mongo_samples_by_version,
    update_mlwh_filtered_positive_fields_batched,
)
from crawler.constants import (
    FIELD_PLATE_BARCODE,
    FIELD_ROOT_SAMPLE_ID,
    V0_V1_CUTOFF_TIMESTAMP,
    V1_V2_CUTOFF_TIMESTAMP,
    MONGO_DATETIME_FORMAT,
)
from crawler.filtered_positive_identifier import (
    FILTERED_POSITIVE_VERSION_0,
    FILTERED_POSITIVE_VERSION_1,
    FILTERED_POSITIVE_VERSION_2,
)

start_datetime = datetime.strptime("201209_0000", MONGO_DATETIME_FORMAT)
end_datetime = datetime.strptime("201217_0000", MONGO_DATETIME_FORMAT)

# ----- mongo_samples_by_date tests -----


def test_mongo_samples_by_date_error_getting_samples(config):
    with patch(
        "migrations.helpers.update_legacy_filtered_positives_helper.create_mongo_client",
        side_effect=ValueError("Boom!"),
    ):
        with pytest.raises(ValueError):
            mongo_samples_by_date(config, start_datetime, end_datetime)


def test_mongo_samples_by_date_returns_correct_samples(config, filtered_positive_testing_samples):
    result = mongo_samples_by_date(config, start_datetime, end_datetime)
    expected_samples = filtered_positive_testing_samples[-3:]

    assert result == expected_samples


# ----- filtered_positive_fields_set tests -----


def test_filtered_positive_fields_set_returns_true_with_fields_set(
    config,
    filtered_positive_testing_samples,
):
    assert filtered_positive_fields_set(config, start_datetime, end_datetime) is True


def test_filtered_positive_fields_set_returns_false_with_no_fields_set(
    config,
    filtered_positive_testing_samples_no_version_set,
):
    assert filtered_positive_fields_set(config, start_datetime, end_datetime) is False


# ----- get_cherrypicked_samples_by_date tests -----


def test_get_cherrypicked_samples_by_date_error_creating_engine_returns_none(config):
    with patch(
        "migrations.helpers.update_legacy_filtered_positives_helper.sqlalchemy.create_engine",
        side_effect=ValueError("Boom!"),
    ):
        returned_samples = get_cherrypicked_samples_by_date(
            config, [], [], "1970-01-01 00:00:01", V0_V1_CUTOFF_TIMESTAMP
        )
        assert returned_samples is None


def test_get_cherrypicked_samples_by_date_error_connecting_returns_none(config):
    with patch(
        "migrations.helpers.update_legacy_filtered_positives_helper.sqlalchemy.create_engine"
    ) as mock_sql_engine:
        mock_sql_engine().connect.side_effect = ValueError("Boom!")
        returned_samples = get_cherrypicked_samples_by_date(
            config, [], [], "1970-01-01 00:00:01", V0_V1_CUTOFF_TIMESTAMP
        )
        assert returned_samples is None


def test_get_cherrypicked_samples_by_date_v0_returns_expected(
    config,
    event_wh_data,
    mlwh_sample_stock_resource,
):
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
    config,
    event_wh_data,
    mlwh_sample_stock_resource,
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


# ----- split_mongo_samples_by_version tests -----


def test_split_mongo_samples_by_version_empty_dataframes(unmigrated_mongo_testing_samples):
    cp_samples_df_v0 = pd.DataFrame(np.array([]))
    cp_samples_df_v1 = pd.DataFrame(np.array([]))

    # sanity check
    assert cp_samples_df_v0.empty
    assert cp_samples_df_v1.empty

    samples_by_version = split_mongo_samples_by_version(
        unmigrated_mongo_testing_samples, cp_samples_df_v0, cp_samples_df_v1
    )

    for version, samples in samples_by_version.items():
        if version == FILTERED_POSITIVE_VERSION_0 or version == FILTERED_POSITIVE_VERSION_1:
            assert samples == []
        elif version == FILTERED_POSITIVE_VERSION_2:
            assert samples == unmigrated_mongo_testing_samples
        else:
            raise AssertionError(f"Unexpected version '{version}'")


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
        else:
            raise AssertionError(f"Unexpected version '{version}'")


# ----- test update_mlwh_filtered_positive_fields_batched method -----


def update_mlwh_filtered_positive_fields_batched_batched_return_false_with_no_connection(config):
    with patch("migrations.helpers.update_legacy_filtered_positives_helper.create_mysql_connection") as mock_connection:
        version = "v2"
        update_timestamp = datetime.now()

        mock_connection().is_connected.return_value = False
        result = update_mlwh_filtered_positive_fields_batched(config, [], version, update_timestamp)
        assert result is False


def test_update_mlwh_filtered_positive_fields_batched_raises_with_error_updating_mlwh(config, mlwh_connection, migrated_mongo_testing_samples):
    version = "v2"
    update_timestamp = datetime.now()

    with patch(
        "migrations.helpers.update_legacy_filtered_positives_helper.run_mysql_execute_formatted_query",
        side_effect=Exception("Boom!"),
    ):
        with pytest.raises(Exception):
            update_mlwh_filtered_positive_fields_batched(config, migrated_mongo_testing_samples, version, update_timestamp)


def test_update_mlwh_filtered_positive_fields_batched_calls_to_update_samples(config, mlwh_connection, migrated_mongo_testing_samples, mlwh_samples):
    insert_sql = """\
    INSERT INTO lighthouse_sample (mongodb_id, root_sample_id, rna_id, plate_barcode, coordinate,
    result, filtered_positive, filtered_positive_version, filtered_positive_timestamp)
    VALUES (%(mongodb_id)s, %(root_sample_id)s, %(rna_id)s, %(plate_barcode)s, %(coordinate)s,
    %(result)s, %(filtered_positive)s, %(filtered_positive_version)s,
    %(filtered_positive_timestamp)s)
    """
    cursor = mlwh_connection.cursor()
    cursor.executemany(insert_sql, mlwh_samples)
    cursor.close()
    mlwh_connection.commit()

    # call to update the database with newly filtered positive entries
    update_timestamp = datetime(2020, 6, 23, 14, 40, 8)

    version = "v2"

    result = update_mlwh_filtered_positive_fields_batched(config, migrated_mongo_testing_samples, version, update_timestamp)
    assert result is True

    cursor = mlwh_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM lighthouse_sample")
    sample_count = cursor.fetchone()[0]
    cursor.execute(
        "SELECT filtered_positive, filtered_positive_version, filtered_positive_timestamp FROM "
        "lighthouse_sample WHERE mongodb_id = '1'"
    )
    filtered_positive_sample = cursor.fetchone()
    cursor.execute(
        "SELECT filtered_positive, filtered_positive_version, filtered_positive_timestamp FROM "
        "lighthouse_sample WHERE mongodb_id = '2'"
    )
    filtered_negative_sample = cursor.fetchone()
    cursor.close()

    assert sample_count == 2
    assert filtered_positive_sample[0]
    assert filtered_positive_sample[1] == "v2"
    assert filtered_positive_sample[2] == update_timestamp
    assert not filtered_negative_sample[0]
    assert filtered_negative_sample[1] == "v2"
    assert filtered_negative_sample[2] == update_timestamp
