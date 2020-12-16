from unittest.mock import patch
import builtins

import pytest

from migrations import update_legacy_filtered_positives


@pytest.fixture
def mock_helper_database_updates():
    with patch("migrations.update_legacy_filtered_positives.update_mongo_filtered_positive_fields") as mock_update_mongo:
        with patch("migrations.update_legacy_filtered_positives.update_mlwh_filtered_positive_fields") as mock_update_mlwh:
            yield mock_update_mongo, mock_update_mlwh

@pytest.fixture
def mock_user_input():
    with patch("migrations.update_legacy_filtered_positives.get_input") as mock_user_input:
            yield mock_user_input


# ----- test migration -----


def test_exception_raised_if_user_cancels_migration(mock_user_input, mock_helper_database_updates, filtered_positive_testing_samples_v0):
    with pytest.raises(Exception):
        mock_user_input.return_value="no"
        mock_update_mongo, mock_update_mlwh = mock_helper_database_updates

        update_legacy_filtered_positives.run("crawler.config.integration")

        mock_update_mongo.assert_not_called()
        mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_no_unmigrated_samples_raises_exception():
    with pytest.raises(Exception):
        with patch(
            "migrations.update_legacy_filtered_positives.unmigrated_mongo_samples", 
            return_value=[]
        ):
            mock_update_mongo, mock_update_mlwh = mock_helper_database_updates
            update_legacy_filtered_positives.run("crawler.config.integration")

            mock_update_mongo.assert_not_called()
            mock_update_mlwh.assert_not_called()


def test_update_legacy_filtered_positives_catches_error_connecting_to_mongo(filtered_positive_testing_samples):
    with pytest.raises(Exception):
        with patch(
            "migrations.update_legacy_filtered_positives.unmigrated_mongo_samples",
            side_effect=Exception("Boom!"),
        ):
            update_legacy_filtered_positives.run("crawler.config.integration")


def test_error_connecting_to_mysql_databases_raises_exception(filtered_positive_testing_samples):
    with pytest.raises(Exception):
        with patch(
            "migrations.update_legacy_filtered_positives.get_cherrypicked_samples_by_date",
            side_effect=Exception("Boom!"),
        ):
            update_legacy_filtered_positives.run("crawler.config.integration")


def test_update_legacy_filtered_positives_outputs_success(
    filtered_positive_testing_samples, event_wh_data, mlwh_sample_stock_resource
):
    mock_update_mongo, mock_update_mlwh = mock_helper_database_updates

    # call the migration
    update_legacy_filtered_positives.run("crawler.config.integration")

    assert mock_update_mongo.call_count == 3
    mock_update_mlwh.assert_called_once()
