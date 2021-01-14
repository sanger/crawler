from unittest.mock import patch
import pandas as pd
import pytest
from datetime import datetime
from crawler.filtered_positive_identifier import (
    FILTERED_POSITIVE_VERSION_0,
    FILTERED_POSITIVE_VERSION_1,
    FILTERED_POSITIVE_VERSION_2,
    FilteredPositiveIdentifierV0,
    FilteredPositiveIdentifierV1,
    FilteredPositiveIdentifierV2,
)
from crawler.constants import MONGO_DATETIME_FORMAT
from migrations import update_legacy_filtered_positives

start_date_input = "201209_0000"
end_date_input = "201217_0000"

identifier_v0 = FilteredPositiveIdentifierV0()
identifier_v1 = FilteredPositiveIdentifierV1()
identifier_v2 = FilteredPositiveIdentifierV2()


@pytest.fixture
def mock_helper_database_updates():
    with patch(
        "migrations.update_legacy_filtered_positives.update_mongo_filtered_positive_fields"
    ) as mock_update_mongo:
        with patch(
            "migrations.update_legacy_filtered_positives.update_mlwh_filtered_positive_fields_batched"
        ) as mock_update_mlwh:
            yield mock_update_mongo, mock_update_mlwh


@pytest.fixture
def mock_user_input():
    with patch("migrations.update_legacy_filtered_positives.get_input") as mock_user_input:
        yield mock_user_input


@pytest.fixture
def mock_filtered_positive_fields_set():
    with patch("migrations.update_legacy_filtered_positives.filtered_positive_fields_set") as mock_filtered_positive_fields_set:
        yield mock_filtered_positive_fields_set


@pytest.fixture
def mock_query_helper_functions():
    with patch(
        "migrations.update_legacy_filtered_positives.mongo_samples_by_date"
    ) as mock_mongo_samples_by_date:  # noqa: E501
        with patch(
            "migrations.update_legacy_filtered_positives.get_cherrypicked_samples_by_date"
        ) as mock_get_cherrypicked_samples_by_date:
            yield mock_mongo_samples_by_date, mock_get_cherrypicked_samples_by_date


@pytest.fixture
def mock_extract_required_cp_info():
    with patch(
        "migrations.update_legacy_filtered_positives.extract_required_cp_info"
    ) as mock_extract_required_cp_info:  # noqa: E501
        yield mock_extract_required_cp_info


@pytest.fixture
def mock_split_mongo_samples_by_version():
    with patch(
        "migrations.update_legacy_filtered_positives.split_mongo_samples_by_version"
    ) as mock_split_mongo_samples_by_version:
        yield mock_split_mongo_samples_by_version


@pytest.fixture
def mock_filtered_positive_identifier_by_version():
    with patch(
        "migrations.update_legacy_filtered_positives.filtered_positive_identifier_by_version"
    ) as mock_identifier_by_version:
        yield mock_identifier_by_version


@pytest.fixture
def mock_update_filtered_positive_fields():
    with patch(
        "migrations.update_legacy_filtered_positives.update_filtered_positive_fields"
    ) as mock_update_filtered_positive_fields:
        yield mock_update_filtered_positive_fields


@pytest.fixture
def mock_valid_datetime_string():
    with patch(
        "migrations.update_legacy_filtered_positives.valid_datetime_string"
    ) as mock_valid_datetime_string:
        yield mock_valid_datetime_string


# ----- test migration -----


def test_update_legacy_filtered_positives_exception_raised_if_user_cancels_migration(
    mock_user_input, mock_helper_database_updates, filtered_positive_testing_samples
):

    mock_user_input.return_value = "no"
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates

    update_legacy_filtered_positives.run("crawler.config.integration", start_date_input, end_date_input)

    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_exception_raised_if_user_enters_invalid_input(
    mock_user_input, mock_helper_database_updates, filtered_positive_testing_samples
):
    mock_user_input.return_value = "invalid_input"
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates

    update_legacy_filtered_positives.run("crawler.config.integration", start_date_input, end_date_input)

    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_returns_early_invalid_start_datetime(
    config, mock_helper_database_updates
):
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    start_datetime = "not a real datetime"
    end_datetime = "201017_1200"

    mock_valid_datetime_string.return_value=False
    update_legacy_filtered_positives.run("crawler.config.integration", start_datetime, end_datetime)

    # ensure no database connections/updates are made
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_returns_early_invalid_end_datetime(
    config, mock_helper_database_updates
):
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    start_datetime = "201016_1600"
    end_datetime = "not a real datetime"

    mock_valid_datetime_string.side_effect=[True, False]
    update_legacy_filtered_positives.run("crawler.config.integration", start_datetime, end_datetime)

    # ensure no database connections/updates are made
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_returns_early_datetime_post_fields_set_date(
    config, mock_helper_database_updates
):
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    start_datetime = "201016_1600"
    end_datetime = "201217_1600"

    mock_valid_datetime_string.return_value = True
    update_legacy_filtered_positives.run("crawler.config.integration", start_datetime, end_datetime)

    # ensure no database connections/updates are made
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_returns_early_start_datetime_after_end_datetime(
    config, mock_helper_database_updates
):
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    start_datetime = "201017_1200"
    end_datetime = "201016_1600"
    update_legacy_filtered_positives.run("crawler.config.integration", start_datetime, end_datetime)

    # ensure no database connections/updates are made
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_catches_error_connecting_to_mongo(
    mock_helper_database_updates, mock_filtered_positive_fields_set
):
    with pytest.raises(Exception):
        with patch(
            mock_update_mongo,
            side_effect=Exception("Boom!"),
        ):
            mock_filtered_positive_fields_set.return_value = False
            mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
            update_legacy_filtered_positives.run("crawler.config.integration", start_date_input, end_date_input)

            mock_update_mongo.assert_not_called()
            mock_update_mlwh.assert_not_called()


def test_get_cherrypicked_samples_by_date_error_raises_exception(
    mock_filtered_positive_fields_set,
    mock_helper_database_updates,
    mock_query_helper_functions,
    mock_extract_required_cp_info,
):
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    mock_mongo_samples_by_date, mock_get_cherrypicked_samples_by_date = mock_query_helper_functions

    mock_filtered_positive_fields_set.return_value = False
    mock_mongo_samples_by_date.return_value = [{"plate_barcode": "1"}]
    mock_extract_required_cp_info.return_value = [["id_1"], ["plate_barcode_1"]]
    mock_get_cherrypicked_samples_by_date.side_effect = Exception("Boom!")

    with pytest.raises(Exception):
        update_legacy_filtered_positives.run("crawler.config.integration", start_date_input, end_date_input)
        mock_update_mongo.assert_not_called()
        mock_update_mlwh.assert_not_called()


def test_extract_required_cp_info_error_raises_exception(
    mock_filtered_positive_fields_set,
    mock_helper_database_updates,
    mock_query_helper_functions,
    mock_extract_required_cp_info,
):
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    mock_mongo_samples_by_date, mock_get_cherrypicked_samples_by_date = mock_query_helper_functions

    mock_filtered_positive_fields_set.return_value = False
    mock_mongo_samples_by_date.return_value = [{"plate_barcode": "1"}]
    mock_extract_required_cp_info.side_effect = Exception("Boom!")

    with pytest.raises(Exception):
        update_legacy_filtered_positives.run("crawler.config.integration", start_date_input, end_date_input)
        mock_update_mongo.assert_not_called()
        mock_update_mlwh.assert_not_called()


def test_split_mongo_samples_by_version_error_raises_exception(
    mock_filtered_positive_fields_set,
    mock_helper_database_updates,
    mock_query_helper_functions,
    mock_extract_required_cp_info,
    mock_split_mongo_samples_by_version,
):
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    mock_mongo_samples_by_date, mock_get_cherrypicked_samples_by_date = mock_query_helper_functions

    mock_filtered_positive_fields_set.return_value = False
    mock_mongo_samples_by_date.return_value = [{"plate_barcode": "1"}]
    mock_extract_required_cp_info.return_value = [["id_1"], ["plate_barcode_1"]]
    mock_get_cherrypicked_samples_by_date.return_value = pd.DataFrame({"id": ["s1", "s2"]})

    mock_split_mongo_samples_by_version.side_effect = Exception("Boom!")

    with pytest.raises(Exception):
        update_legacy_filtered_positives.run("crawler.config.integration", start_date_input, end_date_input)
        mock_update_mongo.assert_not_called()
        mock_update_mlwh.assert_not_called()


def test_filtered_positive_indentifier_by_version_error_raises_exception(
    mock_filtered_positive_fields_set,
    mock_helper_database_updates,
    mock_query_helper_functions,
    mock_extract_required_cp_info,
    mock_split_mongo_samples_by_version,
    mock_filtered_positive_identifier_by_version,
):
    v0_samples = [{"plate_barcode": "0"}]
    v1_samples = [{"plate_barcode": "1"}]
    v2_samples = [{"plate_barcode": "2"}]

    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    mock_mongo_samples_by_date, mock_get_cherrypicked_samples_by_date = mock_query_helper_functions

    mock_filtered_positive_fields_set.return_value = False
    mock_mongo_samples_by_date.return_value = [{"plate_barcode": "1"}]
    mock_extract_required_cp_info.return_value = [["id_1"], ["plate_barcode_1"]]
    mock_get_cherrypicked_samples_by_date.return_value = pd.DataFrame({"id": ["s1", "s2"]})

    mock_split_mongo_samples_by_version.return_value = {
        FILTERED_POSITIVE_VERSION_0: v0_samples,
        FILTERED_POSITIVE_VERSION_1: v1_samples,
        FILTERED_POSITIVE_VERSION_2: v2_samples,
    }

    mock_filtered_positive_identifier_by_version.side_effect = ValueError("Boom!")

    with pytest.raises(ValueError):
        update_legacy_filtered_positives.run("crawler.config.integration", start_date_input, end_date_input)
        mock_update_mongo.assert_not_called()
        mock_update_mlwh.assert_not_called()


def test_update_filtered_positive_fields_error_raises_exception(
    mock_filtered_positive_fields_set,
    mock_helper_database_updates,
    mock_query_helper_functions,
    mock_extract_required_cp_info,
    mock_split_mongo_samples_by_version,
    mock_filtered_positive_identifier_by_version,
    mock_update_filtered_positive_fields,
):
    v0_samples = [{"plate_barcode": "0"}]
    v1_samples = [{"plate_barcode": "1"}]
    v2_samples = [{"plate_barcode": "2"}]

    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    mock_mongo_samples_by_date, mock_get_cherrypicked_samples_by_date = mock_query_helper_functions

    mock_filtered_positive_fields_set.return_value = False
    mock_mongo_samples_by_date.return_value = [{"plate_barcode": "1"}]
    mock_extract_required_cp_info.return_value = [["id_1"], ["plate_barcode_1"]]
    mock_get_cherrypicked_samples_by_date.return_value = pd.DataFrame({"id": ["s1", "s2"]})

    mock_split_mongo_samples_by_version.return_value = {
        FILTERED_POSITIVE_VERSION_0: v0_samples,
        FILTERED_POSITIVE_VERSION_1: v1_samples,
        FILTERED_POSITIVE_VERSION_2: v2_samples,
    }
    mock_filtered_positive_identifier_by_version.return_value = FilteredPositiveIdentifierV0()

    mock_update_filtered_positive_fields.side_effect = Exception("Boom!")

    with pytest.raises(Exception):
        update_legacy_filtered_positives.run("crawler.config.integration", start_date_input, end_date_input)
        mock_update_mongo.assert_not_called()
        mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_outputs_success(
    config,
    freezer,
    mock_filtered_positive_fields_set,
    mock_helper_database_updates,
    mock_query_helper_functions,
    mock_extract_required_cp_info,
    mock_split_mongo_samples_by_version,
    mock_filtered_positive_identifier_by_version,
    mock_update_filtered_positive_fields,
):
    update_timestamp = datetime.now()
    v0_samples = [{"plate_barcode": "0"}]
    v1_samples = [{"plate_barcode": "1"}]
    v2_samples = [{"plate_barcode": "2"}]

    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    mock_mongo_samples_by_date, mock_get_cherrypicked_samples_by_date = mock_query_helper_functions
    mock_split_mongo_samples_by_version = mock_split_mongo_samples_by_version

    mock_filtered_positive_fields_set.return_value = False
    mock_mongo_samples_by_date.return_value = [{"plate_barcode": "1"}]
    mock_extract_required_cp_info.return_value = [["id_1"], ["plate_barcode_1"]]
    mock_get_cherrypicked_samples_by_date.return_value = pd.DataFrame({"id": ["s1", "s2"]})

    mock_split_mongo_samples_by_version.return_value = {
        FILTERED_POSITIVE_VERSION_0: v0_samples,
        FILTERED_POSITIVE_VERSION_1: v1_samples,
        FILTERED_POSITIVE_VERSION_2: v2_samples,
    }
    mock_filtered_positive_identifier_by_version.side_effect = [identifier_v0, identifier_v1, identifier_v2]
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = True

    update_legacy_filtered_positives.run("crawler.config.test", start_date_input, end_date_input)

    assert mock_update_filtered_positive_fields.call_count == 3
    mock_update_filtered_positive_fields.assert_any_call(
        identifier_v0, v0_samples, FILTERED_POSITIVE_VERSION_0, update_timestamp
    )  # noqa: E501
    mock_update_filtered_positive_fields.assert_any_call(
        identifier_v1, v1_samples, FILTERED_POSITIVE_VERSION_1, update_timestamp
    )  # noqa: E501
    mock_update_filtered_positive_fields.assert_any_call(
        identifier_v2, v2_samples, FILTERED_POSITIVE_VERSION_2, update_timestamp
    )  # noqa: E501

    assert mock_update_mongo.call_count == 3
    mock_update_mongo.assert_any_call(config, v0_samples, FILTERED_POSITIVE_VERSION_0, update_timestamp)
    mock_update_mongo.assert_any_call(config, v1_samples, FILTERED_POSITIVE_VERSION_1, update_timestamp)
    mock_update_mongo.assert_any_call(config, v2_samples, FILTERED_POSITIVE_VERSION_2, update_timestamp)

    assert mock_update_mlwh.call_count == 3
    mock_update_mlwh.assert_any_call(config, v0_samples, FILTERED_POSITIVE_VERSION_0, update_timestamp)
    mock_update_mlwh.assert_any_call(config, v1_samples, FILTERED_POSITIVE_VERSION_1, update_timestamp)
    mock_update_mlwh.assert_any_call(config, v2_samples, FILTERED_POSITIVE_VERSION_2, update_timestamp)


def test_update_legacy_filtered_positives_successful_if_user_chooses_to_continue(
    config,
    freezer,
    mock_user_input,
    mock_filtered_positive_fields_set,
    mock_helper_database_updates,
    mock_query_helper_functions,
    mock_extract_required_cp_info,
    mock_split_mongo_samples_by_version,
    mock_filtered_positive_identifier_by_version,
    mock_update_filtered_positive_fields,
):
    update_timestamp = datetime.now()
    v0_samples = [{"plate_barcode": "0"}]
    v1_samples = [{"plate_barcode": "1"}]
    v2_samples = [{"plate_barcode": "2"}]

    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    mock_mongo_samples_by_date, mock_get_cherrypicked_samples_by_date = mock_query_helper_functions
    mock_split_mongo_samples_by_version = mock_split_mongo_samples_by_version

    mock_filtered_positive_fields_set.return_value = True
    mock_user_input.return_value = "yes"
    mock_mongo_samples_by_date.return_value = [{"plate_barcode": "1"}]
    mock_extract_required_cp_info.return_value = [["id_1"], ["plate_barcode_1"]]
    mock_get_cherrypicked_samples_by_date.return_value = pd.DataFrame({"id": ["s1", "s2"]})
    mock_split_mongo_samples_by_version.return_value = {
        FILTERED_POSITIVE_VERSION_0: v0_samples,
        FILTERED_POSITIVE_VERSION_1: v1_samples,
        FILTERED_POSITIVE_VERSION_2: v2_samples,
    }
    mock_filtered_positive_identifier_by_version.side_effect = [identifier_v0, identifier_v1, identifier_v2]
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = True

    update_legacy_filtered_positives.run("crawler.config.test", start_date_input, end_date_input)

    assert mock_update_filtered_positive_fields.call_count == 3
    mock_update_filtered_positive_fields.assert_any_call(
        identifier_v0, v0_samples, FILTERED_POSITIVE_VERSION_0, update_timestamp
    )  # noqa: E501
    mock_update_filtered_positive_fields.assert_any_call(
        identifier_v1, v1_samples, FILTERED_POSITIVE_VERSION_1, update_timestamp
    )  # noqa: E501
    mock_update_filtered_positive_fields.assert_any_call(
        identifier_v2, v2_samples, FILTERED_POSITIVE_VERSION_2, update_timestamp
    )  # noqa: E501

    assert mock_update_mongo.call_count == 3
    mock_update_mongo.assert_any_call(config, v0_samples, FILTERED_POSITIVE_VERSION_0, update_timestamp)
    mock_update_mongo.assert_any_call(config, v1_samples, FILTERED_POSITIVE_VERSION_1, update_timestamp)
    mock_update_mongo.assert_any_call(config, v2_samples, FILTERED_POSITIVE_VERSION_2, update_timestamp)

    assert mock_update_mlwh.call_count == 3
    mock_update_mlwh.assert_any_call(config, v0_samples, FILTERED_POSITIVE_VERSION_0, update_timestamp)
    mock_update_mlwh.assert_any_call(config, v1_samples, FILTERED_POSITIVE_VERSION_1, update_timestamp)
    mock_update_mlwh.assert_any_call(config, v2_samples, FILTERED_POSITIVE_VERSION_2, update_timestamp)
