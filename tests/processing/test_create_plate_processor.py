from typing import NamedTuple
from unittest.mock import ANY, MagicMock, patch

import pytest

from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import (
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
    RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
    RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
)
from crawler.exceptions import TransientRabbitError
from crawler.processing.create_plate_processor import CreatePlateProcessor
from crawler.processing.messages.create_plate_message import CreatePlateMessage
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackError
from tests.testing_objects import CREATE_PLATE_MESSAGE


class EncodedMessage(NamedTuple):
    body: bytes
    version: str


ENCODED_MESSAGE = EncodedMessage(body=b'{"key": "value"}', version="1")


@pytest.fixture
def mock_logger():
    with patch("crawler.processing.create_plate_processor.LOGGER") as logger:
        yield logger


@pytest.fixture
def message():
    message = MagicMock()
    message.message = CREATE_PLATE_MESSAGE

    return message


@pytest.fixture
def mock_validator():
    with patch("crawler.processing.create_plate_processor.CreatePlateValidator") as validator:
        yield validator


@pytest.fixture
def mock_avro_encoder():
    with patch("crawler.processing.create_plate_processor.AvroEncoder") as avro_encoder:
        avro_encoder.return_value.encode.return_value = ENCODED_MESSAGE
        yield avro_encoder


@pytest.fixture
def subject(config, mock_avro_encoder):
    return CreatePlateProcessor(MagicMock(), MagicMock(), config)


def test_constructor_creates_appropriate_encoder(mock_avro_encoder):
    schema_registry = MagicMock()
    CreatePlateProcessor(schema_registry, MagicMock(), MagicMock())

    mock_avro_encoder.assert_called_once_with(schema_registry, RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK)


def test_process_creates_a_create_message_object(subject, message):
    with patch("crawler.processing.create_plate_processor.CreatePlateMessage") as create_plate_message:
        subject.process(message)

    create_plate_message.assert_called_once_with(message.message)


def test_process_uses_validator(subject, mock_validator):
    with patch("crawler.processing.create_plate_processor.CreatePlateMessage") as create_plate_message:
        subject.process(MagicMock())

    mock_validator.assert_called_once_with(create_plate_message.return_value, subject._config)
    mock_validator.return_value.validate.assert_called_once()


def test_process_when_no_issues_found(subject):
    with patch("crawler.processing.create_plate_processor.CreatePlateMessage") as create_plate_message:
        with patch(
            "crawler.processing.create_plate_processor.CreatePlateProcessor._publish_feedback"
        ) as publish_feedback:
            result = subject.process(MagicMock())

    publish_feedback.assert_called_once_with(create_plate_message.return_value)
    assert result is True


def test_process_when_transient_error(subject, mock_logger, mock_validator):
    transient_error = TransientRabbitError("Test transient error")
    mock_validator.return_value.validate.side_effect = transient_error

    with pytest.raises(TransientRabbitError) as ex_info:
        subject.process(MagicMock())

    mock_logger.error.assert_called_once()
    assert ex_info.value == transient_error


def test_process_when_another_exception(subject, mock_logger, mock_validator):
    another_exception = KeyError("key")
    mock_validator.return_value.validate.side_effect = another_exception
    with patch("crawler.processing.create_plate_processor.CreatePlateMessage") as create_plate_message:
        with patch(
            "crawler.processing.create_plate_processor.CreatePlateProcessor._publish_feedback"
        ) as publish_feedback:
            result = subject.process(MagicMock())

    mock_logger.error.assert_called_once()
    create_plate_message.return_value.add_error.assert_called_once_with(
        origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING, description=ANY
    )
    publish_feedback.assert_called_once_with(create_plate_message.return_value)
    assert result is False


def test_publish_feedback_encodes_valid_message(subject, mock_avro_encoder):
    create_message = CreatePlateMessage(CREATE_PLATE_MESSAGE)
    subject._publish_feedback(create_message)

    mock_avro_encoder.return_value.encode.assert_called_once()
    feedback_message = mock_avro_encoder.return_value.encode.call_args.args[0][0]
    assert feedback_message["sourceMessageUuid"] == "b01aa0ad-7b19-4f94-87e9-70d74fb8783c"
    assert feedback_message["countOfTotalSamples"] == 3
    assert feedback_message["countOfValidSamples"] == 0  # We haven't validated the message
    assert feedback_message["operationWasErrorFree"] is True
    assert feedback_message["errors"] == []


def test_publish_feedback_publishes_valid_message(subject):
    subject._publish_feedback(MagicMock())

    subject._basic_publisher.publish_message.assert_called_once_with(
        RABBITMQ_FEEDBACK_EXCHANGE,
        RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
        ENCODED_MESSAGE.body,
        RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
        ENCODED_MESSAGE.version,
    )


@pytest.mark.parametrize(
    "message_errors",
    [
        ([CreateFeedbackError(origin="message_error_1", description="desc_1")]),
        (
            [
                CreateFeedbackError(origin="message_error_1", description="desc_1"),
                CreateFeedbackError(origin="message_error_2", description="desc_2"),
            ]
        ),
    ],
)
def test_publish_feedback_encodes_errors(subject, message, mock_avro_encoder, message_errors):
    create_message = MagicMock()
    create_message.errors = message_errors
    subject._publish_feedback(create_message)

    mock_avro_encoder.return_value.encode.assert_called_once()
    feedback_message = mock_avro_encoder.return_value.encode.call_args.args[0][0]
    assert feedback_message["operationWasErrorFree"] is False
    assert feedback_message["errors"] == message_errors
