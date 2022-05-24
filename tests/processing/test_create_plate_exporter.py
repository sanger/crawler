import copy
from datetime import datetime
from unittest.mock import ANY, MagicMock, patch

import pytest

from crawler.constants import (
    COLLECTION_IMPORTS,
    COLLECTION_SOURCE_PLATES,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGO_LAB_ID,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
)
from crawler.db.mongo import get_mongo_collection
from crawler.exceptions import TransientRabbitError
from crawler.processing.create_plate_exporter import CreatePlateExporter
from crawler.rabbit.messages.create_plate_message import (
    FIELD_LAB_ID,
    FIELD_PLATE,
    FIELD_PLATE_BARCODE,
    CreatePlateError,
    CreatePlateMessage,
    ErrorType,
)
from tests.testing_objects import CREATE_PLATE_MESSAGE


@pytest.fixture
def logger():
    with patch("crawler.processing.create_plate_exporter.LOGGER") as logger:
        yield logger


@pytest.fixture
def create_plate_message():
    return CreatePlateMessage(copy.deepcopy(CREATE_PLATE_MESSAGE))


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

    # Also this still doesn't raise an error against the message
    assert create_plate_message.has_errors is False


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


def test_export_to_mongo_logs_error_correctly_on_exception(subject, logger, mongo_database):
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

    logger.exception.assert_called_once_with(timeout_error)


def test_record_import_creates_a_valid_import_record(freezer, subject, mongo_database, create_plate_message, centre):
    _, mongo_database = mongo_database

    create_plate_message.centre_config = centre.centre_config  # Simulate validation setting the centre config.
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

    with patch("crawler.processing.create_plate_exporter.create_import_record") as create_import_record:
        create_import_record.side_effect = raised_exception
        subject.record_import()

    logger.exception.assert_called_once_with(raised_exception)
