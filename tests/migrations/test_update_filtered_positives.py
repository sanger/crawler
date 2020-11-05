from unittest.mock import patch, MagicMock
import pytest

from migrations.helpers.update_filtered_positives_helper import (
    pending_plate_barcodes_from_dart,
    positive_result_samples_from_mongo,
    update_filtered_positives,
)
from crawler.constants import (
    COLLECTION_SAMPLES,
    FIELD_RESULT,
    FIELD_PLATE_BARCODE,
    POSITIVE_RESULT_VALUE,
)

# ----- test fixture helpers -----

@pytest.fixture
def dart_conn():
    with patch('migrations.helpers.update_filtered_positives_helper.create_dart_sql_server_conn') as mock_connect:
        yield mock_connect

@pytest.fixture
def print_exception():
    with patch('migrations.helpers.update_filtered_positives_helper.print_exception') as mock_print_exception:
        yield mock_print_exception

@pytest.fixture
def mongo_client():
    with patch('migrations.helpers.update_filtered_positives_helper.create_mongo_client') as mock_client:
        yield mock_client

@pytest.fixture
def mongo_db():
    with patch('migrations.helpers.update_filtered_positives_helper.get_mongo_db') as mock_db:
        yield mock_db

@pytest.fixture
def mongo_collection():
    with patch('migrations.helpers.update_filtered_positives_helper.get_mongo_collection') as mock_collection:
        yield mock_collection

@pytest.fixture
def print_status():
    with patch('migrations.helpers.update_filtered_positives_helper.print_processing_status') as mock_print_status:
        yield mock_print_status

# ----- test pending_plate_barcodes_from_dart method -----

def test_pending_plate_barcodes_from_dart_throws_for_error_generating_connection(config, dart_conn):
    dart_conn.side_effect = Exception('Boom!')
    with pytest.raises(Exception):
        pending_plate_barcodes_from_dart(config)

def test_pending_plate_barcodes_from_dart_throws_for_no_connection(config, dart_conn):
    dart_conn.return_value = None
    with pytest.raises(ValueError):
        pending_plate_barcodes_from_dart(config)

def test_pending_plate_barcodes_from_dart_throws_for_error_generating_cursor(config, dart_conn):
    dart_conn().cursor = MagicMock(side_effect = NotImplementedError('Boom!'))
    with pytest.raises(NotImplementedError):
        pending_plate_barcodes_from_dart(config)

def test_pending_plate_barcodes_from_dart_handles_error_executing_statement(config, dart_conn, print_exception):
    dart_conn().cursor().execute = MagicMock(side_effect = Exception('Boom!'))
    pending_plate_barcodes_from_dart(config)
    print_exception.assert_called_once()

def test_pending_plate_barcodes_from_dart_handles_error_committing(config, dart_conn, print_exception):
    dart_conn().cursor().commit.side_effect = Exception('Boom!')
    pending_plate_barcodes_from_dart(config)
    print_exception.assert_called_once()

def test_pending_plate_barcodes_from_dart_returns_expected_plate_barcodes(config, dart_conn):
    expected_plate_barcodes = ['ABC123', '123ABC', 'abcdef']
    dart_conn().cursor().commit.return_value = expected_plate_barcodes
    result = pending_plate_barcodes_from_dart(config)

    dart_conn().cursor().execute.assert_called_once_with('{CALL dbo.plDART_PendingPlates}')
    assert result == expected_plate_barcodes

# ----- test positive_result_samples_from_mongo method -----

def test_positive_result_samples_from_mongo_throws_for_errors_creating_client(config, mongo_client):
    mongo_client.side_effect = Exception('Boom!')
    with pytest.raises(Exception):
        positive_result_samples_from_mongo(config, [])

def test_positive_result_samples_from_mongo_throws_for_error_creating_db(config, mongo_db):
    mongo_db.side_effect = NotImplementedError('Boom!')
    with pytest.raises(NotImplementedError):
        positive_result_samples_from_mongo(config, [])

def test_positive_result_samples_from_mongo_throws_for_error_getting_collection(config, mongo_collection):
    mongo_collection.side_effect = ValueError('Boom!')
    with pytest.raises(ValueError):
        positive_result_samples_from_mongo(config, [])

def test_positive_result_samples_from_mongo_throws_for_error_finding_samples(config, mongo_collection):
    mongo_collection().find.side_effect = Exception('Boom!')
    with pytest.raises(Exception):
        positive_result_samples_from_mongo(config, [])

def test_positive_result_samples_from_mongo_returns_expected_samples(config, mongo_db, mongo_collection):
    plate_barcodes = ['ABC123', '123ABC', 'abcdef']
    positive_result_samples_from_mongo(config, plate_barcodes)

    # first check we make expected calls
    mongo_collection.assert_called_once_with(mongo_db(), COLLECTION_SAMPLES)
    mongo_collection().find.assert_called_once_with({
        FIELD_RESULT: { '$eq': POSITIVE_RESULT_VALUE },
        FIELD_PLATE_BARCODE: { '$in': plate_barcodes }
    })

    # then check expected results are returned
    expected_samples = ['sample1', 'sample2']
    mongo_collection().find.return_value = expected_samples
    result = positive_result_samples_from_mongo(config, plate_barcodes)
    assert result == expected_samples

# ----- test update_filtered_positives method -----

# TODO - add more tests as more of the method is implemented

def test_update_filtered_positives_catches_error_fetching_from_dart(config, dart_conn, print_exception, print_status):
    dart_conn.side_effect = ValueError('Boom!')
    update_filtered_positives(config)

    print_exception.assert_called_once()
    print_status.assert_called_once_with(False, False, False)
    # TODO - test no database update methods are called?

def test_update_filtered_positives_aborts_with_no_plates_fetched_from_dart(config, dart_conn, print_status):
    dart_conn().cursor().commit.return_value = []
    update_filtered_positives(config)

    print_status.assert_called_once_with(False, False, False)
    # TODO - test no database update methods are called?

def test_update_filtered_positives_catches_error_fetching_from_mongo(config, dart_conn, mongo_client, print_exception, print_status):
    dart_conn().cursor().commit.return_value = ['ABC123']
    mongo_client.side_effect = NotImplementedError('Boom!')
    update_filtered_positives(config)

    print_exception.assert_called_once()
    print_status.assert_called_once_with(False, False, False)
    # TODO - test no database update methods are called?

def test_update_filtered_positives_aborts_with_no_positive_samples_fetched_from_mongo(config, dart_conn, mongo_collection, print_status):
    dart_conn().cursor().commit.return_value = ['ABC123']
    mongo_collection().find.return_value = []
    update_filtered_positives(config)

    print_status.assert_called_once_with(False, False, False)
    # TODO - test no database update methods are called?