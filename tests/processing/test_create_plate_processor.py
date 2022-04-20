from unittest.mock import ANY, MagicMock, Mock, call, patch

import pytest

from crawler.constants import RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK
from crawler.processing.create_plate_processor import CreatePlateProcessor
from tests.testing_objects import CREATE_PLATE_MESSAGE


@pytest.fixture
def mock_logger():
    with patch("crawler.processing.create_plate_processor.LOGGER") as logger:
        yield logger


@pytest.fixture
def message():
    message = MagicMock()
    message.message.return_value = CREATE_PLATE_MESSAGE

    return message


@pytest.fixture
def mock_avro_encoder():
    with patch("crawler.processing.create_plate_processor.AvroEncoder") as avro_encoder:
        yield avro_encoder


@pytest.fixture
def subject(mock_avro_encoder, config):
    return CreatePlateProcessor(MagicMock(), MagicMock(), config)


# @pytest.mark.parametrize("uses_ssl", [True, False])
# def test_connect_provides_correct_parameters(mock_logger, uses_ssl):


def test_constructor_creates_appropriate_encoder(mock_avro_encoder):
    schema_registry = MagicMock()
    CreatePlateProcessor(schema_registry, MagicMock(), MagicMock())

    mock_avro_encoder.assert_called_once_with(schema_registry, RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK)


def test_process_calls_validate_for_message(subject, message):
    with patch("crawler.processing.create_plate_processor.CreatePlateProcessor._validate_message") as validate_message:
        subject.process(message)

    validate_message.assert_called_once_with(message)
