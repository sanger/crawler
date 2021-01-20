from datetime import datetime
from unittest.mock import patch, MagicMock, PropertyMock

import pytest

from crawler.helpers.general_helpers import get_config
from migrations import update_filtered_positives

# ----- test fixture helpers -----


@pytest.fixture
def mock_helper_imports():
    with patch("migrations.update_filtered_positives.pending_plate_barcodes_from_dart") as mock_get_plate_barcodes:
        with patch(
            "migrations.update_filtered_positives.positive_result_samples_from_mongo"
        ) as mock_get_positive_samples:
            yield mock_get_plate_barcodes, mock_get_positive_samples


@pytest.fixture
def mock_remove_cherrypicked():
    with patch("migrations.update_filtered_positives.remove_cherrypicked_samples") as mock_remove_cp:
        yield mock_remove_cp


@pytest.fixture
def mock_update_positives():
    with patch("migrations.update_filtered_positives.update_filtered_positive_fields") as mock_udpate:
        yield mock_udpate


@pytest.fixture
def mock_helper_database_updates():
    with patch("migrations.update_filtered_positives.update_mongo_filtered_positive_fields") as mock_update_mongo:
        with patch("migrations.update_filtered_positives.update_mlwh_filtered_positive_fields") as mock_update_mlwh:
            with patch("migrations.update_filtered_positives.update_dart_fields") as mock_update_dart:
                yield mock_update_mongo, mock_update_mlwh, mock_update_dart


# ----- test migration -----


def test_update_filtered_positives_catches_error_fetching_pending_plate_barcodes(
    mock_helper_imports, mock_helper_database_updates
):
    mock_get_plate_barcodes, _ = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock fetching pending plate barcodes to throw
    mock_get_plate_barcodes.side_effect = ValueError("Boom!")

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure no databases are updated
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_aborts_with_no_pending_plate_barcodes(
    mock_helper_imports, mock_helper_database_updates
):
    mock_get_plate_barcodes, _ = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock to return no pending plate barcodes
    mock_get_plate_barcodes.return_value = []

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure that no databases are updated
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_catches_error_pending_positive_samples(
    mock_helper_imports, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a pending plate, but getting associated positive samples to throw
    mock_get_plate_barcodes.return_value = ["ABC123"]
    mock_get_positive_samples.side_effect = NotImplementedError("Boom!")

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure that no databases are updated
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_aborts_with_no_positive_samples_fetched_from_mongo(
    mock_helper_imports, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a pending plate, but no associated positive samples
    mock_get_plate_barcodes.return_value = ["barcode with no matching sample"]
    mock_get_positive_samples.return_value = []

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure that no databases are updated
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_omitting_dart_catches_error_pending_positive_samples(
    mock_helper_imports, mock_helper_database_updates
):
    _, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock getting positive samples to throw
    mock_get_positive_samples.side_effect = NotImplementedError("Boom!")

    # call the migration
    update_filtered_positives.run("crawler.config.integration", True)

    # ensure that no databases are updated
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_omitting_dart_aborts_with_no_positive_samples_fetched_from_mongo(
    mock_helper_imports, mock_helper_database_updates
):
    _, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock no positive samples
    mock_get_positive_samples.return_value = []

    # call the migration
    update_filtered_positives.run("crawler.config.integration", True)

    # ensure that no databases are updated
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_catches_error_removing_cherrypicked_samples(
    mock_helper_imports, mock_remove_cherrypicked, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock removing cherrypicked samples to throw
    mock_get_plate_barcodes.return_value = ["123", "456"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}, {"plate_barcode": "456"}]
    mock_remove_cherrypicked.side_effect = NotImplementedError("Boom!")

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure that no databases are updated
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_aborts_with_no_non_cherrypicked_samples(
    mock_helper_imports, mock_remove_cherrypicked, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock removing cherrypicked samples to throw
    mock_get_plate_barcodes.return_value = ["123", "456"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}, {"plate_barcode": "456"}]
    mock_remove_cherrypicked.return_value = []

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure that no databases are updated
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_catches_error_determining_filtered_positive_results(
    mock_helper_imports, mock_remove_cherrypicked, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock determining the filtered positive fields to throw
    mock_get_plate_barcodes.return_value = ["123", "456"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}, {"plate_barcode": "456"}]
    mock_remove_cherrypicked.return_value = [{"plate_barcode": "123"}]
    mock_update_positives.side_effect = NotImplementedError("Boom!")

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure that no databases are updated
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_catches_error_updating_samples_in_mongo(
    mock_helper_imports, mock_remove_cherrypicked, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock updating the samples in mongo to throw
    mock_get_plate_barcodes.return_value = ["123", "456"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}, {"plate_barcode": "456"}]
    mock_remove_cherrypicked.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.side_effect = NotImplementedError("Boom!")

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_aborts_failing_updating_samples_in_mongo(
    mock_helper_imports, mock_remove_cherrypicked, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock updating the samples in mongo to fail
    mock_get_plate_barcodes.return_value = ["123", "456"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}, {"plate_barcode": "456"}]
    mock_remove_cherrypicked.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.return_value = False

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_catched_error_updating_samples_in_mlwh(
    mock_helper_imports, mock_remove_cherrypicked, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock updating the samples in mlwh to throw
    mock_get_plate_barcodes.return_value = ["123", "456"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}, {"plate_barcode": "456"}]
    mock_remove_cherrypicked.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.return_value = True
    mock_update_mlwh.side_effect = NotImplementedError("Boom!")

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_aborts_failing_updating_samples_in_mlwh(
    mock_helper_imports, mock_remove_cherrypicked, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock updating the samples in mlwh to fail
    mock_get_plate_barcodes.return_value = ["123", "456"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}, {"plate_barcode": "456"}]
    mock_remove_cherrypicked.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = False

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_catches_error_updating_samples_in_dart(
    mock_helper_imports, mock_remove_cherrypicked, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock updating the samples in dart to throw
    mock_get_plate_barcodes.return_value = ["123", "456"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}, {"plate_barcode": "456"}]
    mock_remove_cherrypicked.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = True
    mock_update_dart.side_effect = NotImplementedError("Boom!")

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_called_once()


def test_update_filtered_positives_catches_aborts_failing_updating_samples_in_dart(
    mock_helper_imports, mock_remove_cherrypicked, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock updating the samples in dart to fail
    mock_get_plate_barcodes.return_value = ["123", "456"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}, {"plate_barcode": "456"}]
    mock_remove_cherrypicked.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = True
    mock_update_dart.return_value = False

    # call the migration
    update_filtered_positives.run("crawler.config.integration")

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_called_once()


def test_update_filtered_positives_outputs_success(
    mock_helper_imports, mock_remove_cherrypicked, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a successful update
    mock_get_plate_barcodes.return_value = ["123", "456"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}, {"plate_barcode": "456"}]
    non_cp_samples = [{"plate_barcode": "123"}]
    mock_remove_cherrypicked.return_value = non_cp_samples
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = True
    mock_update_dart.return_value = True

    version = "v2.3"
    mock_pos_id = MagicMock()
    type(mock_pos_id).version = PropertyMock(return_value=version)
    with patch("migrations.update_filtered_positives.current_filtered_positive_identifier", return_value=mock_pos_id):
        with patch("migrations.update_filtered_positives.datetime") as mock_datetime:
            timestamp = datetime.now()
            mock_datetime.now.return_value = timestamp

            # call the migration
            update_filtered_positives.run("crawler.config.integration")

            # ensure expected database calls
            config, _ = get_config("crawler.config.integration")
            mock_update_mongo.assert_called_once()
            mock_update_mongo.assert_called_with(config, non_cp_samples, version, timestamp)
            mock_update_mlwh.assert_called_once()
            mock_update_mlwh.assert_called_with(config, non_cp_samples)
            mock_update_dart.assert_called_once()
            mock_update_dart.assert_called_with(config, non_cp_samples)


def test_update_filtered_positives_omitting_dart_outputs_success(
    mock_helper_imports, mock_remove_cherrypicked, mock_update_positives, mock_helper_database_updates
):
    _, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a successful update
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}, {"plate_barcode": "456"}]
    non_cp_samples = [{"plate_barcode": "123"}]
    mock_remove_cherrypicked.return_value = non_cp_samples
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = True
    mock_update_dart.return_value = True

    version = "v2.3"
    mock_pos_id = MagicMock()
    type(mock_pos_id).version = PropertyMock(return_value=version)
    with patch("migrations.update_filtered_positives.current_filtered_positive_identifier", return_value=mock_pos_id):
        with patch("migrations.update_filtered_positives.datetime") as mock_datetime:
            timestamp = datetime.now()
            mock_datetime.now.return_value = timestamp

            # call the migration
            update_filtered_positives.run("crawler.config.integration", True)

            # ensure expected database calls
            config, _ = get_config("crawler.config.integration")
            mock_update_mongo.assert_called_once()
            mock_update_mongo.assert_called_with(config, non_cp_samples, version, timestamp)
            mock_update_mlwh.assert_called_once()
            mock_update_mlwh.assert_called_with(config, non_cp_samples)
            mock_update_dart.assert_not_called()
