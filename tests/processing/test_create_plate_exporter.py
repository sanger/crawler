from unittest.mock import MagicMock

import pytest

from crawler.processing.create_plate_exporter import CreatePlateExporter
from crawler.rabbit.messages.create_plate_message import CreatePlateMessage
from tests.testing_objects import CREATE_PLATE_MESSAGE


@pytest.fixture
def message():
    message = MagicMock()
    message.message = CREATE_PLATE_MESSAGE

    return message


@pytest.fixture
def create_plate_message(message):
    return CreatePlateMessage(message)


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
