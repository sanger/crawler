from unittest.mock import patch, MagicMock
import pytest
from datetime import datetime

from migrations.helpers.update_filtered_positives_helper import (
    pending_plate_barcodes_from_dart,
    positive_result_samples_from_mongo,
    update_filtered_positive_fields,
    update_mongo_filtered_positive_fields,
    update_mlwh_filtered_positive_fields,
    update_filtered_positives,
    biomek_labclass_by_centre_name,
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
    FIELD_ROOT_SAMPLE_ID,
    FIELD_RNA_ID,
    FIELD_COORDINATE,
    MLWH_MONGODB_ID,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_PLATE_BARCODE,
    MLWH_RNA_ID,
    MLWH_COORDINATE,
    MLWH_RESULT,
    MLWH_FILTERED_POSITIVE,
    MLWH_FILTERED_POSITIVE_VERSION,
    MLWH_FILTERED_POSITIVE_TIMESTAMP,
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
def mock_update_mongo():
    with patch('migrations.helpers.update_filtered_positives_helper.update_mongo_filtered_positive_fields') as mock_update:
        yield mock_update

@pytest.fixture
def mock_update_mlwh():
    with patch('migrations.helpers.update_filtered_positives_helper.update_mlwh_filtered_positive_fields') as mock_update:
        yield mock_update

@pytest.fixture
def mock_update_dart():
    with patch('migrations.helpers.update_filtered_positives_helper.update_samples_in_dart') as mock_update:
        yield mock_update

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

# ----- test update_mongo_filtered_positive_fields method -----

def test_update_mongo_filtered_positive_fields_raises_with_error_updating_mongo(config, mock_mongo_collection):
    mock_mongo_collection().update_many.side_effect = ValueError('Boom!')
    with pytest.raises(ValueError):
        update_mongo_filtered_positive_fields(config, [], 'v2.3', None)

def test_update_mongo_filtered_positive_fields_updates_expected_samples(config, testing_samples, samples_collection_accessor):
    version = 'v2.3'
    timestamp = datetime.now()
    updated_samples = testing_samples[:3]
    updated_samples[0][FIELD_FILTERED_POSITIVE] = True
    updated_samples[1][FIELD_FILTERED_POSITIVE] = False
    updated_samples[2][FIELD_FILTERED_POSITIVE] = False

    result = update_mongo_filtered_positive_fields(config, updated_samples, version, timestamp)
    assert result == True

    assert samples_collection_accessor.count() == len(testing_samples)
    # ensure samples in mongo are updated as expected
    for sample in samples_collection_accessor.find({ FIELD_MONGODB_ID: updated_samples[0][FIELD_MONGODB_ID] }):
        assert sample[FIELD_FILTERED_POSITIVE] == True
        assert sample[FIELD_FILTERED_POSITIVE_VERSION] == version
        assert sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] is not None

    for sample in samples_collection_accessor.find({ FIELD_MONGODB_ID: { "$in": [updated_samples[1][FIELD_MONGODB_ID], updated_samples[2][FIELD_MONGODB_ID]] } }):
        assert sample[FIELD_FILTERED_POSITIVE] == False
        assert sample[FIELD_FILTERED_POSITIVE_VERSION] == version
        assert sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] is not None

# ----- test update_mlwh_filtered_positive_fields method -----

def test_update_mlwh_filtered_positive_fields_return_false_with_no_connection(config):
    with patch('migrations.helpers.update_filtered_positives_helper.create_mysql_connection') as mock_connection:
        mock_connection().is_connected.return_value = False
        result = update_mlwh_filtered_positive_fields(config, [])
        assert result == False

def test_update_mlwh_filtered_positive_fields_raises_with_error_updating_mlwh(config, mlwh_connection):
    with patch('migrations.helpers.update_filtered_positives_helper.run_mysql_executemany_query', side_effect = NotImplementedError('Boom!')):
        with pytest.raises(NotImplementedError):
           update_mlwh_filtered_positive_fields(config, [])

def test_update_mlwh_filtered_positive_fields_calls_to_update_samples(config, mlwh_connection):
    # populate the mlwh database with existing entries
    mlwh_samples = [{
        MLWH_MONGODB_ID: '1',
        MLWH_COORDINATE: 'A1',
        MLWH_PLATE_BARCODE: '123',
        MLWH_ROOT_SAMPLE_ID: 'MCM001',
        MLWH_RNA_ID: 'AAA123',
        MLWH_RESULT: POSITIVE_RESULT_VALUE,
        MLWH_FILTERED_POSITIVE: None,
        MLWH_FILTERED_POSITIVE_VERSION: None,
        MLWH_FILTERED_POSITIVE_TIMESTAMP: None,
    },
    {
        MLWH_MONGODB_ID: '2',
        MLWH_COORDINATE: 'B1',
        MLWH_PLATE_BARCODE: '123',
        MLWH_ROOT_SAMPLE_ID: 'MCM002',
        MLWH_RNA_ID: 'BBB123',
        MLWH_RESULT: POSITIVE_RESULT_VALUE,
        MLWH_FILTERED_POSITIVE: True,
        MLWH_FILTERED_POSITIVE_VERSION: 'v1.0',
        MLWH_FILTERED_POSITIVE_TIMESTAMP: datetime(2020, 4, 23, 14, 40, 8)
    }]
    insert_sql = """\
    INSERT INTO lighthouse_sample (mongodb_id, root_sample_id, rna_id, plate_barcode, coordinate, result, filtered_positive, filtered_positive_version, filtered_positive_timestamp)
    VALUES (%(mongodb_id)s, %(root_sample_id)s, %(rna_id)s, %(plate_barcode)s, %(coordinate)s, %(result)s, %(filtered_positive)s, %(filtered_positive_version)s, %(filtered_positive_timestamp)s)
    """
    cursor = mlwh_connection.cursor()
    cursor.executemany(insert_sql, mlwh_samples)
    cursor.close()
    mlwh_connection.commit()

    # call to update the database with newly filtered positive entries
    update_timestamp = datetime(2020, 6, 23, 14, 40, 8)
    mongo_samples = [{
        FIELD_MONGODB_ID: '1',
        FIELD_COORDINATE: "A01",
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM001",
        FIELD_RNA_ID: 'AAA123',
        FIELD_FILTERED_POSITIVE: True,
        FIELD_FILTERED_POSITIVE_VERSION: 'v2.3',
        FIELD_FILTERED_POSITIVE_TIMESTAMP: update_timestamp
    },
    {
        FIELD_MONGODB_ID: '2',
        FIELD_COORDINATE: "B01",
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM002",
        FIELD_RNA_ID: 'BBB123',
        FIELD_FILTERED_POSITIVE: False,
        FIELD_FILTERED_POSITIVE_VERSION: 'v2.3',
        FIELD_FILTERED_POSITIVE_TIMESTAMP: update_timestamp
    }]

    result = update_mlwh_filtered_positive_fields(config, mongo_samples)
    assert result == True

    cursor = mlwh_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM lighthouse_sample")
    sample_count = cursor.fetchone()[0]
    cursor.execute("SELECT filtered_positive, filtered_positive_version, filtered_positive_timestamp FROM lighthouse_sample WHERE mongodb_id = '1'")
    filtered_positive_sample = cursor.fetchone()
    cursor.execute("SELECT filtered_positive, filtered_positive_version, filtered_positive_timestamp FROM lighthouse_sample WHERE mongodb_id = '2'")
    filtered_negative_sample = cursor.fetchone()
    cursor.close()

    assert sample_count == 2
    assert filtered_positive_sample[0] == True
    assert filtered_positive_sample[1] == 'v2.3'
    assert filtered_positive_sample[2] == update_timestamp
    assert filtered_negative_sample[0] == False
    assert filtered_negative_sample[1] == 'v2.3'
    assert filtered_negative_sample[2] == update_timestamp

# ----- test biomek_labclass_by_centre_name method -----

def test_biomek_labclass_by_centre_name(config):
    centres = [
        {
            "name": "test centre 1",
            "biomek_labware_class": "test class 1"
        },
        {
            "name": "test centre 2",
            "biomek_labware_class": "test class 2"   
        }
    ]
    labclass_by_name = biomek_labclass_by_centre_name(centres)

    assert len(labclass_by_name.keys()) == 2
    assert labclass_by_name["test centre 1"] == "test class 1"
    assert labclass_by_name["test centre 2"] == "test class 2"

# ----- test update_filtered_positives method -----

# TODO - add more tests as more of the method is implemented

def test_update_filtered_positives_catches_error_fetching_from_dart(config, mock_dart_conn, mock_mongo_collection, mock_print_exception, mock_print_status, mock_update_mongo, mock_update_mlwh, mock_update_dart):
    # mock fetching from DART to throw
    mock_dart_conn.side_effect = ValueError('Boom!')

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs, and that no databases are updated
    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(0, 0, False, False, False)
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()

def test_update_filtered_positives_aborts_with_no_plates_fetched_from_dart(config, mock_dart_conn, mock_mongo_collection, mock_print_status, mock_update_mongo, mock_update_mlwh, mock_update_dart):
    # mock DART to return no pending plates
    mock_dart_conn().cursor().execute().fetchall.return_value = []

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs, and that no databases are updated
    mock_print_status.assert_called_once_with(0, 0, False, False, False)
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()

def test_update_filtered_positives_catches_error_fetching_from_mongo(config, mock_dart_conn, mock_mongo_client, mock_mongo_collection, mock_print_exception, mock_print_status, mock_update_mongo, mock_update_mlwh, mock_update_dart):
    # mock dart to return a pending plate, but mongo to throw
    mock_dart_conn().cursor().execute().fetchall.return_value = ['ABC123']
    mock_mongo_client.side_effect = NotImplementedError('Boom!')

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs, and that no databases are updated
    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(1, 0, False, False, False)
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()

def test_update_filtered_positives_aborts_with_no_positive_samples_fetched_from_mongo(config, mock_dart_conn, mock_mongo_collection, mock_print_status, mock_update_mongo, mock_update_mlwh, mock_update_dart):
    # mock dart to return a pending plate, but mongo to return no samples
    mock_dart_conn().cursor().execute().fetchall.return_value = ['barcode with no matching sample']
    mock_mongo_collection().find.return_value = []

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs, and that no databases are updated
    mock_print_status.assert_called_once_with(1, 0, False, False, False)
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()

def test_update_filtered_positives_catches_error_determining_filtered_positive_results(config, mock_dart_conn, mock_mongo_collection, mock_positive_identifier, mock_print_exception, mock_print_status, mock_update_mongo, mock_update_mlwh, mock_update_dart):
    # mock a single pending plate and sample, but determining the filtered positive fields to throw
    mock_dart_conn().cursor().execute().fetchall.return_value = ['123']
    mock_mongo_collection().find.return_value = [{ FIELD_PLATE_BARCODE: '123' }]
    mock_positive_identifier().is_positive.side_effect = NotImplementedError('Boom!')

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs, and that no databases are updated
    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(1, 1, False, False, False)
    mock_update_mongo.assert_not_called()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()

def test_update_filtered_positives_catches_error_updating_samples_in_mongo(config, mock_dart_conn, mock_mongo_collection, mock_print_exception, mock_print_status, mock_update_mongo, mock_update_mlwh, mock_update_dart):
    # mock a single pending plate and sample, but updating the samples in mongo to throw
    mock_dart_conn().cursor().execute().fetchall.return_value = ['123']
    mock_mongo_collection().find.return_value = [{ FIELD_MONGODB_ID: '1', FIELD_PLATE_BARCODE: '123', FIELD_RESULT: POSITIVE_RESULT_VALUE, FIELD_ROOT_SAMPLE_ID: 'MCM001' }]
    mock_update_mongo.side_effect = NotImplementedError('Boom!')

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs
    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(1, 1, False, False, False)
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_not_called()
    mock_update_dart.assert_not_called()

def test_update_filtered_positives_catched_error_updating_samples_in_mlwh(config, mock_dart_conn, mock_mongo_collection, mock_print_exception, mock_print_status, mock_update_mongo, mock_update_mlwh, mock_update_dart):
    # mock a single pending plate and sample, but updating the samples in mlwh to throw
    mock_dart_conn().cursor().execute().fetchall.return_value = ['123']
    mock_mongo_collection().find.return_value = [{ FIELD_MONGODB_ID: '1', FIELD_PLATE_BARCODE: '123', FIELD_RESULT: POSITIVE_RESULT_VALUE, FIELD_ROOT_SAMPLE_ID: 'MCM001' }]
    mock_update_mongo.return_value = True
    mock_update_mlwh.side_effect = NotImplementedError('Boom!')

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs
    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(1, 1, True, False, False)
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_not_called()

def test_update_filtered_positives_catched_error_updating_samples_in_dart(config, mock_dart_conn, mock_mongo_collection, mock_print_exception, mock_print_status, mock_update_mongo, mock_update_mlwh, mock_update_dart):
    # mock a single pending plate and sample, but updating the samples in dart to throw
    mock_dart_conn().cursor().execute().fetchall.return_value = ['123']
    mock_mongo_collection().find.return_value = [{ FIELD_MONGODB_ID: '1', FIELD_PLATE_BARCODE: '123', FIELD_RESULT: POSITIVE_RESULT_VALUE, FIELD_ROOT_SAMPLE_ID: 'MCM001' }]
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = True
    mock_update_dart.side_effect = NotImplementedError('Boom!')

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs
    mock_print_exception.assert_called_once()
    mock_print_status.assert_called_once_with(1, 1, True, True, False)
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_called_once()

def test_update_filtered_positives_outputs_success(config, mock_dart_conn, mock_mongo_collection, mock_print_exception, mock_print_status, mock_update_mongo, mock_update_mlwh, mock_update_dart):
    # mock a single pending plate and sample, and update success
    mock_dart_conn().cursor().execute().fetchall.return_value = ['123']
    mock_mongo_collection().find.return_value = [{ FIELD_MONGODB_ID: '1', FIELD_PLATE_BARCODE: '123', FIELD_RESULT: POSITIVE_RESULT_VALUE, FIELD_ROOT_SAMPLE_ID: 'MCM001' }]
    mock_update_mongo.return_value = True
    mock_update_mlwh.return_value = True
    mock_update_dart.return_value = True

    # call the migration
    update_filtered_positives(config)

    # ensure expected outputs, and that no databases are updated
    mock_print_exception.assert_not_called()
    mock_print_status.assert_called_once_with(1, 1, True, True, True)
    mock_update_mongo.assert_called_once()
    mock_update_mlwh.assert_called_once()
    mock_update_dart.assert_called_once()