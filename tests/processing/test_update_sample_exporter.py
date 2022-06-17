import copy
from unittest.mock import MagicMock, patch

import pytest

from crawler.processing.update_sample_exporter import UpdateSampleExporter
from crawler.rabbit.messages.update_sample_message import UpdateSampleMessage
from tests.testing_objects import UPDATE_SAMPLE_MESSAGE


@pytest.fixture
def logger():
    with patch("crawler.processing.update_sample_exporter.LOGGER") as logger:
        yield logger


@pytest.fixture
def update_sample_message(centre):
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
