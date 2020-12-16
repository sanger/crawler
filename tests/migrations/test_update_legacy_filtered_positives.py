from unittest.mock import patch
import builtins
import pandas as pd
import pytest

from migrations import update_legacy_filtered_positives


@pytest.fixture
def mock_helper_database_updates():
    with patch(
        "migrations.update_legacy_filtered_positives.update_mongo_filtered_positive_fields"
    ) as mock_update_mongo:
        with patch(
            "migrations.update_legacy_filtered_positives.update_mlwh_filtered_positive_fields"
        ) as mock_update_mlwh:
            yield mock_update_mongo, mock_update_mlwh


@pytest.fixture
def mock_user_input():
    with patch("migrations.update_legacy_filtered_positives.get_input") as mock_user_input:
        yield mock_user_input


@pytest.fixture
def mock_v0_version_set():
    with patch("migrations.update_legacy_filtered_positives.v0_version_set") as mock_v0_version_set:
        yield mock_v0_version_set


@pytest.fixture
def mock_helper_functions():
    with patch("migrations.update_legacy_filtered_positives.unmigrated_mongo_samples") as mock_unmigrated_mongo_samples:
        with patch(
            "migrations.update_legacy_filtered_positives.get_cherrypicked_samples_by_date"
        ) as mock_get_cherrypicked_samples_by_date:
            yield mock_unmigrated_mongo_samples, mock_get_cherrypicked_samples_by_date


@pytest.fixture
def mock_extract_required_cp_info():
    with patch("migrations.update_legacy_filtered_positives.extract_required_cp_info") as mock_extract_required_cp_info:
        yield mock_extract_required_cp_info


# ----- test migration -----


def test_update_legacy_filtered_positives_continues_if_user_continues_with_migration(
    mock_user_input, mock_helper_database_updates, filtered_positive_testing_samples_v0
):
    mock_user_input.return_value = "yes"
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates

    update_legacy_filtered_positives.run("crawler.config.integration")

    assert mock_update_mongo.call_count == 3
    assert mock_update_mlwh.call_count == 1


def test_update_legacy_filtered_positives_exception_raised_if_user_cancels_migration(
    mock_user_input, mock_helper_database_updates, filtered_positive_testing_samples_v0
):
    with pytest.raises(Exception):
        mock_user_input.return_value = "no"
        mock_update_mongo, mock_update_mlwh = mock_helper_database_updates

        update_legacy_filtered_positives.run("crawler.config.integration")

        mock_update_mongo.assert_not_called()
        mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_exception_raised_if_user_enters_invalid_input(
    mock_user_input, mock_helper_database_updates, filtered_positive_testing_samples_v0
):
    with pytest.raises(Exception):
        mock_user_input.return_value = "invalid_input"
        mock_update_mongo, mock_update_mlwh = mock_helper_database_updates

        update_legacy_filtered_positives.run("crawler.config.integration")

        mock_update_mongo.assert_not_called()
        mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_no_unmigrated_samples_raises_exception(
    mock_helper_database_updates, mock_v0_version_set
):
    with pytest.raises(Exception):
        mock_v0_version_set.return_value = False
        mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
        update_legacy_filtered_positives.run("crawler.config.integration")

        mock_update_mongo.assert_not_called()
        mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_catches_error_connecting_to_mongo(
    mock_helper_database_updates, mock_v0_version_set
):
    with pytest.raises(Exception):
        with patch(
            "migrations.update_legacy_filtered_positives.unmigrated_mongo_samples",
            side_effect=Exception("Boom!"),
        ):
            mock_v0_version_set.return_value = False
            mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
            update_legacy_filtered_positives.run("crawler.config.integration")

            mock_update_mongo.assert_not_called()
            mock_update_mlwh.assert_not_called()


def test_error_connecting_to_mysql_databases_raises_exception(
    mock_v0_version_set, mock_helper_database_updates, mock_helper_functions, mock_extract_required_cp_info
):
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    mock_unmigrated_mongo_samples, mock_get_cherrypicked_samples_by_date = mock_helper_functions

    mock_v0_version_set.return_value = False
    mock_unmigrated_mongo_samples.return_value = [{"plate_barcode": "1"}]
    mock_extract_required_cp_info.return_value = [["id_1"], ["plate_barcode_1"]]
    mock_get_cherrypicked_samples_by_date.side_effect = Exception("Boom!")

    with pytest.raises(Exception):
        update_legacy_filtered_positives.run("crawler.config.integration")
        mock_update_mongo.assert_not_called()
        mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_outputs_success(
    mock_v0_version_set, mock_helper_database_updates, mock_helper_functions, mock_extract_required_cp_info
):
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
    mock_unmigrated_mongo_samples, mock_get_cherrypicked_samples_by_date = mock_helper_functions

    mock_v0_version_set.return_value = False
    mock_unmigrated_mongo_samples.return_value = [{"plate_barcode": "1"}]
    mock_extract_required_cp_info.return_value = [["id_1"], ["plate_barcode_1"]]
    mock_get_cherrypicked_samples_by_date.side_effect = Exception("Boom!")

    update_legacy_filtered_positives.run("crawler.config.integration")
