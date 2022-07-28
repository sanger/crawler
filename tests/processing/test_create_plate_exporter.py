import copy
from datetime import datetime
from unittest.mock import ANY, MagicMock, patch

import pytest
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

from crawler.constants import (
    DART_STATE_NO_PLATE,
    DART_STATE_NO_PROP,
    DART_STATE_PENDING,
    DART_STATE_PICKABLE,
    FIELD_COORDINATE,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGO_LAB_ID,
    FIELD_MONGO_MESSAGE_UUID,
    FIELD_MONGO_RESULT,
    FIELD_MONGO_RNA_ID,
    FIELD_MONGO_ROOT_SAMPLE_ID,
    FIELD_MONGO_SAMPLE_INDEX,
    FIELD_SOURCE,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
)
from crawler.exceptions import TransientRabbitError
from crawler.processing.create_plate_exporter import CreatePlateExporter
from crawler.rabbit.messages.create_plate_message import (
    FIELD_LAB_ID,
    FIELD_PLATE,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SAMPLES,
    CreatePlateError,
    CreatePlateMessage,
    ErrorType,
)
from tests.testing_objects import CREATE_PLATE_MESSAGE

MONGO_SAMPLES = [
    {
        FIELD_MONGO_LAB_ID: "CPTD",
        FIELD_MONGO_ROOT_SAMPLE_ID: sample[FIELD_ROOT_SAMPLE_ID],
        FIELD_MONGO_RNA_ID: sample[FIELD_RNA_ID],
        FIELD_MONGO_RESULT: sample[FIELD_RESULT].capitalize(),
    }
    for sample in CREATE_PLATE_MESSAGE[FIELD_PLATE][FIELD_SAMPLES]
]


@pytest.fixture
def logger():
    with patch("crawler.processing.create_plate_exporter.LOGGER") as logger:
        yield logger


@pytest.fixture()
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


def test_export_to_mongo_puts_a_source_plate_in_mongo(subject, source_plates_collection_accessor):
    assert source_plates_collection_accessor.count_documents({"barcode": "PLATE-001"}) == 0

    subject.export_to_mongo()

    assert source_plates_collection_accessor.count_documents({"barcode": "PLATE-001"}) == 1


def test_export_to_mongo_puts_samples_in_mongo(subject, samples_collection_accessor):
    assert samples_collection_accessor.count_documents({}) == 0

    subject.export_to_mongo()

    assert samples_collection_accessor.count_documents({}) == 3
    assert (
        samples_collection_accessor.count_documents(
            {
                FIELD_MONGO_MESSAGE_UUID: "CREATE_PLATE_UUID",
                FIELD_MONGO_LAB_ID: "CPTD",
                FIELD_SOURCE: "Alderley",
            }
        )
        == 3
    )
    assert (
        samples_collection_accessor.count_documents(
            {FIELD_MONGO_SAMPLE_INDEX: 1, FIELD_LH_SAMPLE_UUID: "UUID_001", FIELD_COORDINATE: "A01"}
        )
        == 1
    )
    assert (
        samples_collection_accessor.count_documents(
            {FIELD_MONGO_SAMPLE_INDEX: 2, FIELD_LH_SAMPLE_UUID: "UUID_002", FIELD_COORDINATE: "E06"}
        )
        == 1
    )
    assert (
        samples_collection_accessor.count_documents(
            {FIELD_MONGO_SAMPLE_INDEX: 3, FIELD_LH_SAMPLE_UUID: "UUID_003", FIELD_COORDINATE: "H12"}
        )
        == 1
    )


def test_export_to_mongo_sets_the_source_plate_uuid(subject, source_plates_collection_accessor):
    subject.export_to_mongo()

    source_plate = source_plates_collection_accessor.find_one({"barcode": "PLATE-001"})
    plate_uuid = source_plate and source_plate[FIELD_LH_SOURCE_PLATE_UUID]

    assert subject._plate_uuid == plate_uuid


def test_export_to_mongo_puts_a_source_plate_in_mongo_only_once(subject, source_plates_collection_accessor):
    assert source_plates_collection_accessor.count_documents({"barcode": "PLATE-001"}) == 0

    subject.export_to_mongo()
    assert source_plates_collection_accessor.count_documents({"barcode": "PLATE-001"}) == 1

    subject.export_to_mongo()
    assert source_plates_collection_accessor.count_documents({"barcode": "PLATE-001"}) == 1  # Still only 1


def test_export_to_mongo_adds_an_error_when_source_plate_exists_for_another_lab_id(
    subject, source_plates_collection_accessor
):
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
    assert source_plates_collection_accessor.count_documents({FIELD_MONGO_LAB_ID: "NULL"}) == 0


def test_export_to_mongo_logs_error_correctly_on_source_plate_exception(
    subject, logger, samples_collection_accessor, source_plates_collection_accessor
):
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

    assert samples_collection_accessor.count_documents({}) == 0
    assert source_plates_collection_accessor.count_documents({}) == 0

    logger.exception.assert_called_once_with(timeout_error)


def test_export_to_mongo_logs_error_correctly_on_samples_exception(
    subject, logger, samples_collection_accessor, source_plates_collection_accessor
):
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

    assert samples_collection_accessor.count_documents({}) == 0
    assert source_plates_collection_accessor.count_documents({}) == 0

    logger.exception.assert_called_once_with(timeout_error)


@pytest.mark.parametrize("samples_collection_accessor", [[MONGO_SAMPLES[1]]], indirect=True)
def test_export_to_mongo_reverts_the_transaction_when_duplicate_samples_inserted(
    subject, samples_collection_accessor, source_plates_collection_accessor
):
    # Insert sample 2 into the collection already
    subject._message._body[FIELD_PLATE][FIELD_SAMPLES]

    # Run the export
    subject.export_to_mongo()

    print(samples_collection_accessor)
    # No documents were inserted in either collection
    assert samples_collection_accessor.count_documents({}) == 1  # Just the one we added to the fixture
    assert source_plates_collection_accessor.count_documents({}) == 0


@pytest.mark.parametrize("samples_collection_accessor", [[MONGO_SAMPLES[0], MONGO_SAMPLES[2]]], indirect=True)
def test_export_to_mongo_creates_appropriate_error_when_duplicate_samples_inserted(
    subject, samples_collection_accessor
):
    # Run the export
    subject.export_to_mongo()

    # Check that both samples were flagged as already existing
    assert len(subject._message.feedback_errors) == 2
    error1 = subject._message.feedback_errors[0]
    assert error1["typeId"] == 8
    assert error1["origin"] == "sample"
    assert error1["sampleUuid"] == "UUID_001"
    assert "UUID_001" in error1["description"]
    assert "CPTD" in error1["description"]
    assert "R00T-S4MPL3-ID1" in error1["description"]
    assert "RN4-1D-1" in error1["description"]
    assert "Positive" in error1["description"]

    error2 = subject._message.feedback_errors[1]
    assert error2["typeId"] == 8
    assert error2["origin"] == "sample"
    assert error2["sampleUuid"] == "UUID_003"
    assert "UUID_003" in error2["description"]
    assert "CPTD" in error2["description"]
    assert "R00T-S4MPL3-ID3" in error2["description"]
    assert "RN4-1D-3" in error2["description"]
    assert "Void" in error2["description"]


def test_export_to_mongo_logs_error_correctly_on_bulk_write_error_with_mix_of_errors(
    subject, logger, samples_collection_accessor, source_plates_collection_accessor
):
    bulk_write_error = BulkWriteError(
        {"errorLabels": [], "writeErrors": [{"code": 11000, "op": MagicMock()}, {"code": 999}]}
    )

    with patch.object(Collection, "insert_many", side_effect=bulk_write_error):
        with pytest.raises(TransientRabbitError):
            subject.export_to_mongo()

    # No documents were inserted in either collection
    assert samples_collection_accessor.count_documents({}) == 0
    assert source_plates_collection_accessor.count_documents({}) == 0

    logger.critical.assert_called_once()
    logger.exception.assert_called_once_with(bulk_write_error)


def test_export_to_mongo_logs_error_correctly_on_bulk_write_error_without_duplicates(
    subject, logger, samples_collection_accessor, source_plates_collection_accessor
):
    bulk_write_error = BulkWriteError({"errorLabels": [], "writeErrors": [{"code": 999}]})

    with patch.object(Collection, "insert_many", side_effect=bulk_write_error):
        with pytest.raises(TransientRabbitError):
            subject.export_to_mongo()

    # No documents were inserted in either collection
    assert samples_collection_accessor.count_documents({}) == 0
    assert source_plates_collection_accessor.count_documents({}) == 0

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

    assert logger.debug.call_count == 3
    assert "DART database inserts completed successfully" in logger.debug.call_args_list[1].args[0]


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


def test_record_import_creates_a_valid_import_record(freezer, subject, imports_collection_accessor):
    subject._samples_inserted = 3  # Simulate inserting all the records.

    subject.record_import()

    assert (
        imports_collection_accessor.count_documents(
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
