from typing import NamedTuple
from unittest.mock import ANY, MagicMock, PropertyMock, patch

import pytest

from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import (
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
    RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
    RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
)
from crawler.exceptions import TransientRabbitError
from crawler.processing.create_plate_processor import CreatePlateProcessor
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
def mock_validator():
    with patch("crawler.processing.create_plate_processor.CreatePlateValidator") as validator:
        type(validator).message = PropertyMock(return_value=CREATE_PLATE_MESSAGE)
        type(validator).total_samples = PropertyMock(return_value=96)
        type(validator).valid_samples = PropertyMock(return_value=96)
        type(validator).errors = PropertyMock(return_value=[])
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


def test_process_uses_validator(subject, mock_validator):
    message = MagicMock()
    subject.process(message)

    mock_validator.assert_called_once_with(message, subject._config)
    mock_validator.return_value.validate.assert_called_once()


def test_process_when_no_issues_found(subject, mock_validator):
    with patch("crawler.processing.create_plate_processor.CreatePlateProcessor._publish_feedback") as publish_feedback:
        result = subject.process(MagicMock())

    publish_feedback.assert_called_once_with(mock_validator.return_value)
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
    with patch("crawler.processing.create_plate_processor.CreatePlateProcessor._publish_feedback") as publish_feedback:
        result = subject.process(MagicMock())

    mock_logger.error.assert_called_once()
    publish_feedback.assert_called_once_with(mock_validator.return_value, additional_errors=[ANY])
    additional_error = publish_feedback.call_args.kwargs["additional_errors"][0]
    assert additional_error["origin"] == RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING
    assert "unhandled error" in additional_error["description"].lower()
    assert result is False


def test_publish_feedback_encodes_valid_message(subject, mock_validator, mock_avro_encoder):
    subject._publish_feedback(mock_validator)

    mock_avro_encoder.return_value.encode.assert_called_once()
    feedback_message = mock_avro_encoder.return_value.encode.call_args.args[0][0]
    assert feedback_message["sourceMessageUuid"] == "b01aa0ad-7b19-4f94-87e9-70d74fb8783c"
    assert feedback_message["countOfTotalSamples"] == 96
    assert feedback_message["countOfValidSamples"] == 96
    assert feedback_message["operationWasErrorFree"] is True
    assert feedback_message["errors"] == []


def test_publish_feedback_publishes_valid_message(subject, mock_validator):
    subject._publish_feedback(mock_validator)

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
@pytest.mark.parametrize(
    "additional_errors",
    [
        ([CreateFeedbackError(origin="additional_error_1", description="desc_1")]),
        (
            [
                CreateFeedbackError(origin="additional_error_1", description="desc_1"),
                CreateFeedbackError(origin="additional_error_2", description="desc_2"),
            ]
        ),
    ],
)
def test_publish_feedback_encodes_errors(subject, mock_validator, mock_avro_encoder, message_errors, additional_errors):
    type(mock_validator).errors = PropertyMock(return_value=message_errors)

    subject._publish_feedback(mock_validator, additional_errors)

    mock_avro_encoder.return_value.encode.assert_called_once()
    feedback_message = mock_avro_encoder.return_value.encode.call_args.args[0][0]
    assert feedback_message["operationWasErrorFree"] is False
    assert feedback_message["errors"] == message_errors + additional_errors
