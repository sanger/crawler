import copy
from unittest.mock import MagicMock, patch

import pytest

from crawler.constants import COLLECTION_SOURCE_PLATES, FIELD_LH_SOURCE_PLATE_UUID
from crawler.db.mongo import get_mongo_collection
from crawler.exceptions import TransientRabbitError
from crawler.processing.create_plate_exporter import CreatePlateExporter
from crawler.rabbit.messages.create_plate_message import FIELD_LAB_ID, FIELD_PLATE, CreatePlateMessage
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


def test_export_to_mongo_adds_an_error_when_source_plate_exists_for_another_lab_id(subject, create_plate_message):
    # Get the source plate added once
    subject.export_to_mongo()

    assert create_plate_message.has_errors is False

    subject._message._body[FIELD_PLATE][FIELD_LAB_ID] = "NULL"
    subject.export_to_mongo()

    assert create_plate_message.has_errors is True


def test_export_to_mongo_logs_error_correctly_on_exception(subject, logger):
    timeout_error = TimeoutError()

    with patch("crawler.processing.create_plate_exporter.get_mongo_collection") as get_mongo_collection:
        get_mongo_collection.side_effect = timeout_error

        with pytest.raises(TransientRabbitError) as ex_info:
            subject.export_to_mongo()

    assert (
        ex_info.value.message == "There was an error updating MongoDB while exporting plate with barcode 'PLATE-001'."
    )

    logger.critical.assert_called_once()
    log_message = logger.critical.call_args.args[0]
    assert "PLATE-001" in log_message
    assert str(timeout_error) in log_message

    logger.exception.assert_called_once_with(timeout_error)
