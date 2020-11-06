from unittest.mock import patch, MagicMock
import pytest
from datetime import datetime

from migrations.helpers.update_filtered_positives_helper import (
    pending_plate_barcodes_from_dart,
    positive_result_samples_from_mongo,
    update_filtered_positive_fields,
    update_filtered_positives,
)
from crawler.constants import (
    COLLECTION_SAMPLES,
    FIELD_RESULT,
    FIELD_PLATE_BARCODE,
    POSITIVE_RESULT_VALUE,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_FILTERED_POSITIVE_TIMESTAMP
)

# ----- test fixture helpers -----

@pytest.fixture
def mock_dart_conn():
    with patch('migrations.helpers.update_filtered_positives_helper.create_dart_sql_server_conn') as mock_connect:
        yield mock_connect

@pytest.fixture
def mock_print_exception():
    with patch('migrations.helpers.update_filtered_positives_helper.print_exception') as mock_print:
        yield mock_print

@pytest.fixture
def mock_mongo_client():
    with patch('migrations.helpers.update_filtered_positives_helper.create_mongo_client') as mock_client:
        yield mock_client

@pytest.fixture
def mock_mongo_collection():
    with patch('migrations.helpers.update_filtered_positives_helper.get_mongo_collection') as mock_collection:
        yield mock_collection

@pytest.fixture
def mock_print_status():
    with patch('migrations.helpers.update_filtered_positives_helper.print_processing_status') as mock_print:
        yield mock_print

@pytest.fixture
def mock_positive_identifier():
    with patch('migrations.helpers.update_filtered_positives_helper.FilteredPositiveIdentifier') as mock_identifier:
        mock_identifier.is_positive.return_value = True
        mock_identifier.current_version.return_value = 'v2.3'
        yield mock_identifier

# ----- test pending_plate_barcodes_from_dart method -----

def test_pending_plate_barcodes_from_dart_throws_for_error_generating_connection(config, mock_dart_conn):
    mock_dart_conn.side_effect = Exception('Boom!')
    with pytest.raises(Exception):
        pending_plate_barcodes_from_dart(config)

def test_pending_plate_barcodes_from_dart_throws_for_no_connection(config, mock_dart_conn):
    mock_dart_conn.return_value = None
    with pytest.raises(ValueError):
        pending_plate_barcodes_from_dart(config)

def test_pending_plate_barcodes_from_dart_throws_for_error_generating_cursor(config, mock_dart_conn):
    mock_dart_conn().cursor = MagicMock(side_effect = NotImplementedError('Boom!'))
    with pytest.raises(NotImplementedError):
        pending_plate_barcodes_from_dart(config)

def test_pending_plate_barcodes_from_dart_handles_error_executing_statement(config, mock_dart_conn, mock_print_exception):
    mock_dart_conn().cursor().execute = MagicMock(side_effect = Exception('Boom!'))
    pending_plate_barcodes_from_dart(config)
    mock_print_exception.assert_called_once()

def test_pending_plate_barcodes_from_dart_handles_error_committing(config, mock_dart_conn, mock_print_exception):
    mock_dart_conn().cursor().commit.side_effect = Exception('Boom!')
    pending_plate_barcodes_from_dart(config)
    mock_print_exception.assert_called_once()

def test_pending_plate_barcodes_from_dart_returns_expected_plate_barcodes(config, mock_dart_conn):
    expected_plate_barcodes = ['ABC123', '123ABC', 'abcdef']
    mock_dart_conn().cursor().commit.return_value = expected_plate_barcodes
    result = pending_plate_barcodes_from_dart(config)

    mock_dart_conn().cursor().execute.assert_called_once_with('{CALL dbo.plDART_PendingPlates}')
    assert result == expected_plate_barcodes

# ----- test positive_result_samples_from_mongo method -----

def test_positive_result_samples_from_mongo_throws_for_errors_creating_client(config, mock_mongo_client):
    mock_mongo_client.side_effect = Exception('Boom!')
    with pytest.raises(Exception):
        positive_result_samples_from_mongo(config, [])

def test_positive_result_samples_from_mongo_throws_for_error_creating_db(config):
    with patch('migrations.helpers.update_filtered_positives_helper.get_mongo_db') as mongo_db:
        mongo_db.side_effect = NotImplementedError('Boom!')
        with pytest.raises(NotImplementedError):
            positive_result_samples_from_mongo(config, [])

def test_positive_result_samples_from_mongo_throws_for_error_getting_collection(config, mock_mongo_collection):
    mock_mongo_collection.side_effect = ValueError('Boom!')
    with pytest.raises(ValueError):
        positive_result_samples_from_mongo(config, [])

def test_positive_result_samples_from_mongo_throws_for_error_finding_samples(config, mock_mongo_collection):
    mock_mongo_collection().find.side_effect = Exception('Boom!')
    with pytest.raises(Exception):
        positive_result_samples_from_mongo(config, [])

def test_positive_result_samples_from_mongo_returns_expected_samples(config, testing_samples):
    plate_barcodes = ['123']
    expected_samples = testing_samples[:1] # only the first sample is positive, with matching plate barcode
    result = positive_result_samples_from_mongo(config, plate_barcodes)
    assert result == expected_samples

# ----- test update_filtered_positive_fields method -----

def test_update_filtered_positive_fields_assigns_expected_filtered_positive_fields(mock_positive_identifier):
    samples = [{}, {}]
    timestamp = datetime.now()

    update_filtered_positive_fields(mock_positive_identifier, samples, timestamp)
    for sample in samples:
        assert sample[FIELD_FILTERED_POSITIVE] == True
        assert sample[FIELD_FILTERED_POSITIVE_VERSION] == 'v2.3'
        assert sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] == timestamp


# ----- test update_filtered_positives method -----

# TODO - add more tests as more of the method is implemented

def test_update_filtered_positives_catches_error_fetching_from_dart(config, mock_dart_conn, mock_print_exception, mock_print_status):
    mock_dart_conn.side_effect = ValueError('Boom!')
    update_filtered_positives(config)

    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(False, False, False)
    # TODO - test no database update methods are called?

def test_update_filtered_positives_aborts_with_no_plates_fetched_from_dart(config, mock_dart_conn, mock_print_status):
    mock_dart_conn().cursor().commit.return_value = []
    update_filtered_positives(config)

    mock_print_status.assert_called_once_with(False, False, False)
    # TODO - test no database update methods are called?

def test_update_filtered_positives_catches_error_fetching_from_mongo(config, mock_dart_conn, mock_mongo_client, mock_print_exception, mock_print_status):
    mock_dart_conn().cursor().commit.return_value = ['ABC123']
    mock_mongo_client.side_effect = NotImplementedError('Boom!')
    update_filtered_positives(config)

    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(False, False, False)
    # TODO - test no database update methods are called?

def test_update_filtered_positives_aborts_with_no_positive_samples_fetched_from_mongo(config, mock_dart_conn, testing_samples, mock_print_status):
    mock_dart_conn().cursor().commit.return_value = ['barcode with no matching sample']
    update_filtered_positives(config)

    mock_print_status.assert_called_once_with(False, False, False)
    # TODO - test no database update methods are called?

def test_update_filtered_positives_catches_error_determining_filtered_positive_results(config, mock_dart_conn, testing_samples, mock_positive_identifier, mock_print_exception, mock_print_status):
    mock_dart_conn().cursor().commit.return_value = ['123']
    mock_positive_identifier().current_version.side_effect = NotImplementedError('Boom!')
    update_filtered_positives(config)

    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(False, False, False)
    # TODO - test no database update methods are called?