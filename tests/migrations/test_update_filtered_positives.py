import pytest
from unittest.mock import patch
from migrations import update_filtered_positives

# ----- test fixture helpers -----


@pytest.fixture
def mock_helper_imports():
    with patch(
        "migrations.update_filtered_positives.pending_plate_barcodes_from_dart"
    ) as mock_get_plate_barcodes:
        with patch(
            "migrations.update_filtered_positives.positive_result_samples_from_mongo"
        ) as mock_get_positive_samples:
            yield mock_get_plate_barcodes, mock_get_positive_samples


@pytest.fixture
def mock_update_positives():
    with patch(
        "migrations.update_filtered_positives.update_filtered_positive_fields"
    ) as mock_udpate:
        yield mock_udpate


@pytest.fixture
def mock_helper_database_updates():
    with patch(
        "migrations.update_filtered_positives.update_mongo_filtered_positive_fields"
    ) as mock_update_mongo:
        with patch(
            "migrations.update_filtered_positives.update_mlwh_filtered_positive_fields"
        ) as mock_update_mlwh:
            with patch(
                "migrations.update_filtered_positives.update_dart_filtered_positive_fields"
            ) as mock_update_dart:
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
    update_filtered_positives.run()

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
    update_filtered_positives.run()

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
    update_filtered_positives.run()

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
    update_filtered_positives.run()

    # ensure that no databases are updated
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_catches_error_determining_filtered_positive_results(
    mock_helper_imports, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a single pending plate and sample, but determining the filtered positive fields to throw
    mock_get_plate_barcodes.return_value = ["123"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}]
    mock_update_positives.side_effect = NotImplementedError("Boom!")

    # call the migration
    update_filtered_positives.run()

    # ensure that no databases are updated
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_catches_error_updating_samples_in_mongo(
    mock_helper_imports, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a single pending plate and sample, but updating the samples in mongo to throw
    mock_get_plate_barcodes.return_value = ["123"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.side_effect = NotImplementedError("Boom!")

    # call the migration
    update_filtered_positives.run()

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_aborts_failing_updating_samples_in_mongo(
    mock_helper_imports, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a single pending plate and sample, but updating the samples in mongo to fail
    mock_get_plate_barcodes.return_value = ["123"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.return_value = False

    # call the migration
    update_filtered_positives.run()

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_catched_error_updating_samples_in_mlwh(
    mock_helper_imports, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a single pending plate and sample, but updating the samples in mlwh to throw
    mock_get_plate_barcodes.return_value = ["123"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.return_value = True
    mock_update_mlwh.side_effect = NotImplementedError("Boom!")

    # call the migration
    update_filtered_positives.run()

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_aborts_failing_updating_samples_in_mlwh(
    mock_helper_imports, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a single pending plate and sample, but updating the samples in mlwh to fail
    mock_get_plate_barcodes.return_value = ["123"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = False

    # call the migration
    update_filtered_positives.run()

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_not_called()


def test_update_filtered_positives_catches_error_updating_samples_in_dart(
    mock_helper_imports, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a single pending plate and sample, but updating the samples in dart to throw
    mock_get_plate_barcodes.return_value = ["123"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = True
    mock_update_dart.side_effect = NotImplementedError("Boom!")

    # call the migration
    update_filtered_positives.run()

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_called_once()


def test_update_filtered_positives_catches_aborts_failing_updating_samples_in_dart(
    mock_helper_imports, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a single pending plate and sample, but updating the samples in dart to fail
    mock_get_plate_barcodes.return_value = ["123"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = True
    mock_update_dart.return_value = False

    # call the migration
    update_filtered_positives.run()

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_called_once()


def test_update_filtered_positives_outputs_success(
    mock_helper_imports, mock_update_positives, mock_helper_database_updates
):
    mock_get_plate_barcodes, mock_get_positive_samples = mock_helper_imports
    mock_update_mongo, mock_update_mlwh, mock_update_dart = mock_helper_database_updates

    # mock a single pending plate and sample, with update success
    mock_get_plate_barcodes.return_value = ["123"]
    mock_get_positive_samples.return_value = [{"plate_barcode": "123"}]
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = True
    mock_update_dart.return_value = True

    # call the migration
    update_filtered_positives.run()

    # ensure expected database calls
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_called_once()
