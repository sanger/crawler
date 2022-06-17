import copy
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from crawler.constants import COLLECTION_SAMPLES, FIELD_LH_SAMPLE_UUID, FIELD_PLATE_BARCODE, FIELD_UPDATED_AT
from crawler.db.mongo import get_mongo_collection
from crawler.exceptions import TransientRabbitError
from crawler.processing.update_sample_exporter import UpdateSampleExporter
from crawler.rabbit.messages.update_sample_message import ErrorType, UpdateSampleMessage
from tests.testing_objects import UPDATE_SAMPLE_MESSAGE


@pytest.fixture
def logger():
    with patch("crawler.processing.update_sample_exporter.LOGGER") as logger:
        yield logger


@pytest.fixture
def update_sample_message():
    return UpdateSampleMessage(copy.deepcopy(UPDATE_SAMPLE_MESSAGE))


@pytest.fixture
def subject(update_sample_message, config):
    return UpdateSampleExporter(update_sample_message, config)


def test_constructor_stores_arguments_as_instance_variables():
    message = MagicMock()
    config = MagicMock()
    subject = UpdateSampleExporter(message, config)

    assert subject._message == message
    assert subject._config == config


def test_mongo_db_gets_the_mongo_instance(subject, mongo_database):
    _, mongo_database = mongo_database

    assert subject._mongo_db == mongo_database


def test_verify_sample_in_mongo_when_no_sample_in_mongo(subject):
    subject.verify_sample_in_mongo()

    assert len(subject._message.feedback_errors) == 1
    assert subject._message.feedback_errors[0]["typeId"] == ErrorType.ExporterSampleDoesNotExist
    assert subject._plate_barcode is None


def test_verify_sample_in_mongo_when_sample_is_present(subject, mongo_database, update_sample_message):
    _, mongo_database = mongo_database
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    samples_collection.insert_one(
        {
            FIELD_LH_SAMPLE_UUID: "UPDATE_SAMPLE_UUID",
            FIELD_UPDATED_AT: datetime.now() - timedelta(hours=1),
            FIELD_PLATE_BARCODE: "A_PLATE_BARCODE",
        }
    )

    subject.verify_sample_in_mongo()

    assert len(subject._message.feedback_errors) == 0
    assert subject._plate_barcode == "A_PLATE_BARCODE"


def test_verify_sample_in_mongo_when_sample_is_more_recently_updated_than_the_message(
    subject, mongo_database, update_sample_message
):
    _, mongo_database = mongo_database
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    samples_collection.insert_one(
        {
            FIELD_LH_SAMPLE_UUID: "UPDATE_SAMPLE_UUID",
            FIELD_UPDATED_AT: datetime.now() + timedelta(hours=1),
            FIELD_PLATE_BARCODE: "A_PLATE_BARCODE",
        }
    )

    subject.verify_sample_in_mongo()

    assert len(subject._message.feedback_errors) == 1
    assert subject._message.feedback_errors[0]["typeId"] == ErrorType.ExporterMessageOutOfDate
    assert subject._plate_barcode is None


def test_verify(subject, logger):
    timeout_error = TimeoutError()

    with patch("crawler.processing.update_sample_exporter.get_mongo_collection") as get_collection:
        get_collection.side_effect = timeout_error

        with pytest.raises(TransientRabbitError) as ex_info:
            subject.verify_sample_in_mongo()

    logger.critical.assert_called_once()
    logger.exception.assert_called_once_with(timeout_error)
    assert "'UPDATE_SAMPLE_UUID'" in ex_info.value.message
