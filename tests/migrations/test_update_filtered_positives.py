from unittest.mock import patch, MagicMock
import pytest
from datetime import datetime

from migrations.helpers.update_filtered_positives_helper import (
    pending_plate_barcodes_from_dart,
    positive_result_samples_from_mongo,
    update_filtered_positive_fields,
    update_samples_in_mongo,
    update_filtered_positives,
)
from crawler.constants import (
    COLLECTION_SAMPLES,
    FIELD_MONGODB_ID,
    FIELD_RESULT,
    FIELD_PLATE_BARCODE,
    POSITIVE_RESULT_VALUE,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    DART_STATE_PENDING,
)
from crawler.sql_queries import (
    SQL_DART_GET_PLATE_BARCODES,
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

def assert_no_database_updates(mongo_collection):
    mongo_collection().update_many.assert_not_called()
    # TODO - test other dbs aren't called

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

def test_pending_plate_barcodes_from_dart_returns_expected_plate_barcodes(config, mock_dart_conn):
    expected_rows = [('ABC123', ), ('123ABC', ), ('abcdef', )]
    mock_dart_conn().cursor().execute().fetchall.return_value = expected_rows
    result = pending_plate_barcodes_from_dart(config)

    mock_dart_conn().cursor().execute.assert_called_with(SQL_DART_GET_PLATE_BARCODES, DART_STATE_PENDING)
    assert result == ['ABC123', '123ABC', 'abcdef']

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
    version = 'v2.3'

    update_filtered_positive_fields(mock_positive_identifier, samples, version, timestamp)
    for sample in samples:
        assert sample[FIELD_FILTERED_POSITIVE] == True
        assert sample[FIELD_FILTERED_POSITIVE_VERSION] == version
        assert sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] == timestamp

# ----- test update_samples_in_mongo method -----

# TODO - integration testing around the transaction workflow

def test_update_samples_in_mongo_does_update_with_error_updating_mongo(config, mock_mongo_collection):
    mock_mongo_collection().update_many.side_effect = ValueError('Boom!')
    with pytest.raises(ValueError):
        update_samples_in_mongo(config, [], 'v2.3', None)

def test_update_samples_in_mongo_updates_expected_samples(config, testing_samples, samples_collection_accessor):
    version = 'v2.3'
    timestamp = datetime.now()
    updated_samples = testing_samples[:3]
    updated_samples[0][FIELD_FILTERED_POSITIVE] = True
    updated_samples[1][FIELD_FILTERED_POSITIVE] = False
    updated_samples[2][FIELD_FILTERED_POSITIVE] = False

    result = update_samples_in_mongo(config, updated_samples, version, timestamp)
    assert result == True

    # ensure samples in mongo are updated as expected
    for sample in samples_collection_accessor.find({ FIELD_MONGODB_ID: updated_samples[0][FIELD_MONGODB_ID] }):
        assert sample[FIELD_FILTERED_POSITIVE] == True
        assert sample[FIELD_FILTERED_POSITIVE_VERSION] == version
        assert sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] is not None

    for sample in samples_collection_accessor.find({ FIELD_MONGODB_ID: { "$in": [updated_samples[1][FIELD_MONGODB_ID], updated_samples[2][FIELD_MONGODB_ID]] } }):
        assert sample[FIELD_FILTERED_POSITIVE] == False
        assert sample[FIELD_FILTERED_POSITIVE_VERSION] == version
        assert sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] is not None

# ----- test update_filtered_positives method -----

# TODO - add more tests as more of the method is implemented

def test_update_filtered_positives_catches_error_fetching_from_dart(config, mock_dart_conn, mock_mongo_collection, mock_print_exception, mock_print_status):
    # mock fetching from DART to throw
    mock_dart_conn.side_effect = ValueError('Boom!')

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs, and that no databases are updated
    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(0, 0, False, False, False)
    assert_no_database_updates(mock_mongo_collection)

def test_update_filtered_positives_aborts_with_no_plates_fetched_from_dart(config, mock_dart_conn, mock_mongo_collection, mock_print_status):
    # mock DART to return no pending plates
    mock_dart_conn().cursor().execute().fetchall.return_value = []

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs, and that no databases are updated
    mock_print_status.assert_called_once_with(0, 0, False, False, False)
    assert_no_database_updates(mock_mongo_collection)

def test_update_filtered_positives_catches_error_fetching_from_mongo(config, mock_dart_conn, mock_mongo_client, mock_mongo_collection, mock_print_exception, mock_print_status):
    # mock dart to return a pending plate, but mongo to throw
    mock_dart_conn().cursor().execute().fetchall.return_value = ['ABC123']
    mock_mongo_client.side_effect = NotImplementedError('Boom!')

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs, and that no databases are updated
    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(1, 0, False, False, False)
    assert_no_database_updates(mock_mongo_collection)

def test_update_filtered_positives_aborts_with_no_positive_samples_fetched_from_mongo(config, mock_dart_conn, mock_mongo_collection, mock_print_status):
    # mock dart to return a pending plate, but mongo to return no samples
    mock_dart_conn().cursor().execute().fetchall.return_value = ['barcode with no matching sample']
    mock_mongo_collection().find.return_value = []

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs, and that no databases are updated
    mock_print_status.assert_called_once_with(1, 0, False, False, False)
    assert_no_database_updates(mock_mongo_collection)

def test_update_filtered_positives_catches_error_determining_filtered_positive_results(config, mock_dart_conn, mock_mongo_collection, mock_positive_identifier, mock_print_exception, mock_print_status):
    # mock a single pending plate and sample, but determining the filtered positive fields to throw
    mock_dart_conn().cursor().execute().fetchall.return_value = ['123']
    mock_mongo_collection().find.return_value = [{ FIELD_PLATE_BARCODE: '123' }]
    mock_positive_identifier().is_positive.side_effect = NotImplementedError('Boom!')

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs, and that no databases are updated
    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(1, 1, False, False, False)
    assert_no_database_updates(mock_mongo_collection)

def test_update_filtered_positives_catches_error_updating_samples_in_mongo(config, mock_dart_conn, testing_samples, mock_mongo_collection, mock_print_exception, mock_print_status):
    # mock a single pending plate and sample, but updating the samples in mongo to throw
    mock_dart_conn().cursor().execute().fetchall.return_value = ['123']
    mock_mongo_collection().find.return_value = [{ FIELD_PLATE_BARCODE: '123' }]
    with patch('migrations.helpers.update_filtered_positives_helper.update_samples_in_mongo', side_effect = NotImplementedError('Boom!')):
        # call the migration
        update_filtered_positives(config)

        # ensure expected outputs, and that no databases are updated
        mock_print_exception.assert_called_once()
        mock_print_status.assert_called_once_with(1, 1, False, False, False)
        assert_no_database_updates(mock_mongo_collection)

# def test_update_filtered_positives_outputs_success(config):