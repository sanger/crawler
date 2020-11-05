from unittest.mock import patch, MagicMock
import pytest

from migrations.helpers.update_filtered_positives_helper import (
    pending_plate_barcodes_from_dart,
    positive_result_samples_from_mongo,
    update_filtered_positives,
)

# ----- test fixture helpers -----

@pytest.fixture
def dart_conn():
    with patch('migrations.helpers.update_filtered_positives_helper.create_dart_sql_server_conn') as mock_connect:
        yield mock_connect

@pytest.fixture
def print_exception():
    with patch('migrations.helpers.update_filtered_positives_helper.print_exception') as mock_print:
        yield mock_print

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
    dart_conn().cursor().commit = MagicMock(side_effect = Exception('Boom!'))
    pending_plate_barcodes_from_dart(config)
    print_exception.assert_called_once()

def test_pending_plate_barcodes_from_dart_returns_expected_plate_barcodes(config, dart_conn):
    expected_plate_barcodes = ['ABC123', '123ABC', 'abcdef']
    dart_conn().cursor().commit = MagicMock(return_value = expected_plate_barcodes)
    result = pending_plate_barcodes_from_dart(config)
    dart_conn().cursor().execute.assert_called_once_with('{CALL dbo.plDART_PendingPlates}')
    assert result == expected_plate_barcodes
