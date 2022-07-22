import copy
from datetime import datetime
from unittest.mock import ANY, MagicMock, patch

import pytest
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

from crawler.constants import (COLLECTION_IMPORTS, COLLECTION_SAMPLES,
                               COLLECTION_SOURCE_PLATES, DART_STATE_NO_PLATE,
                               DART_STATE_NO_PROP, DART_STATE_PENDING,
                               DART_STATE_PICKABLE, FIELD_COORDINATE,
                               FIELD_LH_SAMPLE_UUID,
                               FIELD_LH_SOURCE_PLATE_UUID, FIELD_MONGO_LAB_ID,
                               FIELD_MONGO_MESSAGE_UUID,
                               FIELD_MONGO_SAMPLE_INDEX, FIELD_SOURCE,
                               RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE)
from crawler.db.mongo import get_mongo_collection
from crawler.exceptions import TransientRabbitError
from crawler.processing.create_plate_exporter import CreatePlateExporter
from crawler.rabbit.messages.create_plate_message import (FIELD_LAB_ID,
                                                          FIELD_PLATE,
                                                          FIELD_PLATE_BARCODE,
                                                          FIELD_SAMPLES,
                                                          CreatePlateError,
                                                          CreatePlateMessage,
                                                          ErrorType)
from tests.testing_objects import CREATE_PLATE_MESSAGE


@pytest.fixture
def logger():
    with patch("crawler.processing.create_plate_exporter.LOGGER") as logger:
        yield logger


@pytest.fixture
def create_plate_message(centre):
    plate_message = CreatePlateMessage(copy.deepcopy(CREATE_PLATE_MESSAGE))
    plate_message.centre_config = centre.centre_config  # Simulate running validation on the message.

    return plate_message


@pytest.fixture
def subject(create_plate_message, config):
    return CreatePlateExporter(create_plate_message, config)


def test_constructor_stores_arguments_as_instance_variables():
    message = MagicMock()
    config = MagicMock()
    subject = CreatePlateExporter(message, config)

    assert subject._message == message
    assert subject._config == config


def test_mongo_db_gets_the_mongo_instance(subject, mongo_database):
    _, mongo_database = mongo_database

    assert subject._mongo_db == mongo_database


def test_export_to_mongo_adds_no_errors_to_the_message(subject, create_plate_message):
    subject.export_to_mongo()

    assert create_plate_message.has_errors is False


def test_export_to_mongo_puts_a_source_plate_in_mongo(subject, mongo_database):
    _, mongo_database = mongo_database

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)

    assert source_plates_collection.count_documents({"barcode": "PLATE-001"}) == 0

    subject.export_to_mongo()

    assert source_plates_collection.count_documents({"barcode": "PLATE-001"}) == 1


def test_export_to_mongo_puts_samples_in_mongo(subject, mongo_database):
    _, mongo_database = mongo_database

    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)

    assert samples_collection.count_documents({}) == 0

    subject.export_to_mongo()

    assert samples_collection.count_documents({}) == 3
    assert (
        samples_collection.count_documents(
            {
                FIELD_MONGO_MESSAGE_UUID: "CREATE_PLATE_UUID",
                FIELD_MONGO_LAB_ID: "CPTD",
                FIELD_SOURCE: "Alderley",
            }
        )
        == 3
    )
    assert (
        samples_collection.count_documents(
            {FIELD_MONGO_SAMPLE_INDEX: 1, FIELD_LH_SAMPLE_UUID: "UUID_001", FIELD_COORDINATE: "A01"}
        )
        == 1
    )
    assert (
        samples_collection.count_documents(
            {FIELD_MONGO_SAMPLE_INDEX: 2, FIELD_LH_SAMPLE_UUID: "UUID_002", FIELD_COORDINATE: "E06"}
        )
        == 1
    )
    assert (
        samples_collection.count_documents(
            {FIELD_MONGO_SAMPLE_INDEX: 3, FIELD_LH_SAMPLE_UUID: "UUID_003", FIELD_COORDINATE: "H12"}
        )
        == 1
    )


def test_export_to_mongo_sets_the_source_plate_uuid(subject, mongo_database):
    _, mongo_database = mongo_database

    subject.export_to_mongo()

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    source_plate = source_plates_collection.find_one({"barcode": "PLATE-001"})
    plate_uuid = source_plate and source_plate[FIELD_LH_SOURCE_PLATE_UUID]

    assert subject._plate_uuid == plate_uuid


def test_export_to_mongo_puts_a_source_plate_in_mongo_only_once(subject, mongo_database, create_plate_message):
    _, mongo_database = mongo_database

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)

    assert source_plates_collection.count_documents({"barcode": "PLATE-001"}) == 0

    subject.export_to_mongo()
    assert source_plates_collection.count_documents({"barcode": "PLATE-001"}) == 1

    subject.export_to_mongo()
    assert source_plates_collection.count_documents({"barcode": "PLATE-001"}) == 1  # Still only 1


def test_export_to_mongo_adds_an_error_when_source_plate_exists_for_another_lab_id(subject, mongo_database):
    _, mongo_database = mongo_database

    # Get the source plate added once
    subject.export_to_mongo()

    subject._message._body[FIELD_PLATE][FIELD_LAB_ID] = "NULL"
    with patch.object(CreatePlateMessage, "add_error") as add_error:
        subject.export_to_mongo()

    add_error.assert_called_once_with(
        CreatePlateError(
            type=ErrorType.ExportingPlateAlreadyExists,
            origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
            description=ANY,
            field=FIELD_LAB_ID,
        )
    )

    # NULL plate was not inserted
    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    assert source_plates_collection.count_documents({FIELD_MONGO_LAB_ID: "NULL"}) == 0


def test_export_to_mongo_logs_error_correctly_on_source_plate_exception(subject, logger, mongo_database):
    _, mongo_database = mongo_database
    timeout_error = TimeoutError()

    with patch("crawler.processing.create_plate_exporter.get_mongo_collection") as get_collection:
        get_collection.side_effect = timeout_error

        with pytest.raises(TransientRabbitError) as ex_info:
            subject.export_to_mongo()

    assert (
        ex_info.value.message == "There was an error updating MongoDB while exporting plate with barcode 'PLATE-001'."
    )

    logger.critical.assert_called_once()
    log_message = logger.critical.call_args.args[0]
    assert "PLATE-001" in log_message
    assert str(timeout_error) in log_message

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    assert source_plates_collection.count_documents({}) == 0

    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    assert samples_collection.count_documents({}) == 0

    logger.exception.assert_called_once_with(timeout_error)


def test_export_to_mongo_logs_error_correctly_on_samples_exception(subject, logger, mongo_database):
    _, mongo_database = mongo_database
    timeout_error = TimeoutError()

    with patch.object(Collection, "insert_many", side_effect=timeout_error):
        with pytest.raises(TransientRabbitError) as ex_info:
            subject.export_to_mongo()

    assert ex_info.value.message == (
        "There was an error updating MongoDB while exporting samples for message UUID 'CREATE_PLATE_UUID'."
    )

    logger.critical.assert_called_once()
    log_message = logger.critical.call_args.args[0]
    assert "CREATE_PLATE_UUID" in log_message
    assert str(timeout_error) in log_message

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    assert source_plates_collection.count_documents({}) == 0

    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    assert samples_collection.count_documents({}) == 0

    logger.exception.assert_called_once_with(timeout_error)


def test_export_to_mongo_reverts_the_transaction_when_duplicate_samples_inserted(subject, mongo_database):
    _, mongo_database = mongo_database

    samples = subject._message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0] = samples[1]
    subject.export_to_mongo()

    # No documents were inserted in either collection
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    assert samples_collection.count_documents({}) == 0

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    assert source_plates_collection.count_documents({}) == 0


def test_export_to_mongo_creates_appropriate_error_when_duplicate_samples_inserted(subject, mongo_database):
    _, mongo_database = mongo_database

    samples = subject._message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0] = samples[1]
    subject.export_to_mongo()

    assert len(subject._message.feedback_errors) == 1
    error = subject._message.feedback_errors[0]
    assert error["typeId"] == 8
    assert error["origin"] == "sample"
    assert error["sampleUuid"] == "UUID_002"
    assert "UUID_002" in error["description"]
    assert "CPTD" in error["description"]
    assert "R00T-S4MPL3-ID2" in error["description"]
    assert "RN4-1D-2" in error["description"]
    assert "Negative" in error["description"]


def test_export_to_mongo_logs_error_correctly_on_bulk_write_error_with_mix_of_errors(subject, mongo_database):
    _, mongo_database = mongo_database
    bulk_write_error = BulkWriteError(
        {"errorLabels": [], "writeErrors": [{"code": 11000, "op": MagicMock()}, {"code": 999}]}
    )

    with patch.object(Collection, "insert_many", side_effect=bulk_write_error):
        subject.export_to_mongo()

    # No documents were inserted in either collection
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    assert samples_collection.count_documents({}) == 0

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    assert source_plates_collection.count_documents({}) == 0


def test_export_to_mongo_logs_error_correctly_on_bulk_write_error_without_duplicates(subject, logger, mongo_database):
    _, mongo_database = mongo_database
    bulk_write_error = BulkWriteError({"errorLabels": [], "writeErrors": [{"code": 999}]})

    with patch.object(Collection, "insert_many", side_effect=bulk_write_error):
        with pytest.raises(TransientRabbitError):
            subject.export_to_mongo()

    logger.critical.assert_called_once()
    logger.exception.assert_called_once_with(bulk_write_error)


def test_export_to_dart_connects_to_the_database(subject, pyodbc_conn):
    subject.export_to_dart()

    pyodbc_conn.assert_called()


def test_export_to_dart_submits_plate(subject, pyodbc_conn):
    with patch("crawler.processing.create_plate_exporter.add_dart_plate_if_doesnt_exist") as add_method:
        subject.export_to_dart()

    add_method.assert_called_once()


def test_export_to_dart_submits_all_samples_when_plate_pending(subject, pyodbc_conn):
    with patch("crawler.processing.create_plate_exporter.add_dart_plate_if_doesnt_exist") as add_plate_method:
        add_plate_method.return_value = DART_STATE_PENDING
        with patch("crawler.processing.create_plate_exporter.add_dart_well_properties_if_positive") as add_method:
            subject.export_to_dart()

    assert add_method.call_count == 3


def test_export_to_dart_commits_to_the_database(subject):
    with patch("crawler.processing.create_plate_exporter.create_dart_sql_server_conn") as connect:
        subject.export_to_dart()

    cursor = connect.return_value.cursor.return_value
    cursor.commit.assert_called_once()


def test_export_to_dart_creates_no_message_errors(subject, pyodbc_conn, logger):
    subject.export_to_dart()

    assert subject._message.has_errors is False

    logger.debug.assert_called_once()
    assert "DART database inserts completed successfully" in logger.debug.call_args.args[0]


@pytest.mark.parametrize("plate_state", [DART_STATE_NO_PLATE, DART_STATE_NO_PROP, DART_STATE_PICKABLE])
def test_export_to_dart_does_not_submit_any_samples_when_plate_not_pending(subject, pyodbc_conn, plate_state):
    with patch("crawler.processing.create_plate_exporter.add_dart_plate_if_doesnt_exist") as add_plate_method:
        add_plate_method.return_value = plate_state
        with patch("crawler.processing.create_plate_exporter.add_dart_well_properties_if_positive") as add_method:
            subject.export_to_dart()

    add_method.assert_not_called()


def test_export_to_dart_handles_no_connection_to_database(subject, logger):
    with patch("crawler.processing.create_plate_exporter.create_dart_sql_server_conn") as connect:
        connect.return_value = None
        subject.export_to_dart()

    logger.critical.assert_called_once()
    assert "Error connecting to DART database" in logger.critical.call_args.args[0]

    assert len(subject._message._textual_errors) == 1
    assert "Error connecting to DART database" in subject._message._textual_errors[0]


def test_export_to_dart_handles_insertion_failures(subject, pyodbc_conn, logger):
    error = Exception("Boom!")

    with patch("crawler.processing.create_plate_exporter.add_dart_plate_if_doesnt_exist") as add_plate_method:
        add_plate_method.side_effect = error
        subject.export_to_dart()

    logger.exception.assert_called_once_with(error)
    logger.critical.assert_called_once()
    assert "DART database inserts failed" in logger.critical.call_args.args[0]

    assert len(subject._message._textual_errors) == 1
    assert "DART database inserts failed" in subject._message._textual_errors[0]


def test_export_to_dart_rolls_back_on_insert_exception(subject):
    with patch("crawler.processing.create_plate_exporter.create_dart_sql_server_conn") as connect:
        with patch("crawler.processing.create_plate_exporter.add_dart_plate_if_doesnt_exist") as add_plate_method:
            add_plate_method.side_effect = Exception("Boom!")
            subject.export_to_dart()

    cursor = connect.return_value.cursor.return_value
    cursor.commit.assert_not_called()
    cursor.rollback.assert_called_once()


def test_record_import_creates_a_valid_import_record(freezer, subject, mongo_database):
    _, mongo_database = mongo_database

    subject._samples_inserted = 3  # Simulate inserting all the records.

    subject.record_import()

    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)

    assert (
        imports_collection.count_documents(
            {
                "date": datetime.utcnow(),  # Time has been frozen for this test.
                "centre_name": "Alderley",
                "csv_file_used": "PLATE-001",
                "number_of_records": 3,
                "errors": ["No errors were reported during processing."],
            }
        )
        == 1
    )


def test_record_import_logs_an_error_if_message_contains_no_plate_barcode(subject, logger, create_plate_message):
    create_plate_message._body[FIELD_PLATE][FIELD_PLATE_BARCODE] = ""

    subject.record_import()

    logger.error.assert_called_once_with(
        "Import record not created for message with UUID 'CREATE_PLATE_UUID' because it doesn't have a plate barcode."
    )


def test_record_import_logs_an_exception_if_getting_mongo_collection_raises(subject, logger):
    raised_exception = Exception()

    with patch("crawler.processing.create_plate_exporter.get_mongo_collection") as get_mongo_collection:
        get_mongo_collection.side_effect = raised_exception
        subject.record_import()

    logger.exception.assert_called_once_with(raised_exception)


def test_record_import_logs_an_exception_if_creating_import_record_raises(subject, logger):
    raised_exception = Exception()

    with patch("crawler.processing.create_plate_exporter.create_mongo_import_record") as create_import_record:
        create_import_record.side_effect = raised_exception
        subject.record_import()

    logger.exception.assert_called_once_with(raised_exception)
