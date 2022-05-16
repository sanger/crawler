import copy
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
from crawler.rabbit.messages.create_plate_message import CreatePlateError, CreatePlateMessage, MessageField
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
def create_plate_message():
    return CreatePlateMessage(copy.deepcopy(CREATE_PLATE_MESSAGE))


@pytest.fixture
def mock_validator():
    with patch("crawler.processing.create_plate_processor.CreatePlateValidator") as validator:
        yield validator


@pytest.fixture
def mock_exporter():
    with patch("crawler.processing.create_plate_processor.CreatePlateExporter") as exporter:
        yield exporter


@pytest.fixture
def mock_avro_encoder():
    with patch("crawler.processing.create_plate_processor.AvroEncoder") as avro_encoder:
        avro_encoder.return_value.encode.return_value = ENCODED_MESSAGE
        yield avro_encoder


@pytest.fixture
def message_wrapper_class():
    with patch("crawler.processing.create_plate_processor.CreatePlateMessage") as message_wrapper_class:
        message_wrapper_class.return_value.message_uuid = MessageField("UUID_FIELD", "UUID")
        message_wrapper_class.return_value.has_errors = False
        yield message_wrapper_class


@pytest.fixture
def subject(config, mock_avro_encoder, mock_validator, mock_exporter):
    return CreatePlateProcessor(MagicMock(), MagicMock(), config)


def test_constructor_creates_appropriate_encoder(mock_avro_encoder):
    schema_registry = MagicMock()
    CreatePlateProcessor(schema_registry, MagicMock(), MagicMock())

    mock_avro_encoder.assert_called_once_with(schema_registry, RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK)


def test_process_creates_a_create_plate_message_object(subject, message_wrapper_class):
    message = MagicMock()
    message.message = "A message body"
    subject.process(message)

    message_wrapper_class.assert_called_once_with("A message body")


def test_process_uses_validator(subject, mock_validator, message_wrapper_class):
    subject.process(MagicMock())
    mock_validator.assert_called_once_with(message_wrapper_class.return_value, subject._config)
    mock_validator.return_value.validate.assert_called_once()


def test_process_uses_exporter(subject, mock_exporter, message_wrapper_class):
    subject.process(MagicMock())
    mock_exporter.assert_called_once_with(message_wrapper_class.return_value, subject._config)
    mock_exporter.return_value.export_to_mongo.assert_called_once()


def test_process_publishes_feedback_when_no_issues_found(subject, mock_validator):
    with patch("crawler.processing.create_plate_processor.CreatePlateMessage") as create_plate_message:
        with patch(
            "crawler.processing.create_plate_processor.CreatePlateProcessor._publish_feedback"
        ) as publish_feedback:
            subject.process(MagicMock())

    publish_feedback.assert_called_once_with(create_plate_message.return_value)


def test_process_returns_true_when_no_issues_found(subject, mock_validator, mock_exporter):
    result = subject.process(MagicMock())

    assert result is True


def test_process_when_transient_error_from_validator(subject, mock_logger, mock_validator):
    transient_error = TransientRabbitError("Test transient error")
    mock_validator.return_value.validate.side_effect = transient_error

    with pytest.raises(TransientRabbitError) as ex_info:
        subject.process(MagicMock())

    mock_logger.error.assert_called_once()
    assert ex_info.value == transient_error


def test_process_when_transient_error_from_exporter(subject, mock_logger, mock_exporter):
    transient_error = TransientRabbitError("Test transient error")
    mock_exporter.return_value.export_to_mongo.side_effect = transient_error

    with pytest.raises(TransientRabbitError) as ex_info:
        subject.process(MagicMock())

    mock_logger.error.assert_called_once()
    assert ex_info.value == transient_error


def test_process_when_another_exception_from_the_validator(subject, mock_logger, mock_validator, message_wrapper_class):
    another_exception = KeyError("key")
    mock_validator.return_value.validate.side_effect = another_exception
    with patch("crawler.processing.create_plate_processor.CreatePlateProcessor._publish_feedback") as publish_feedback:
        result = subject.process(MagicMock())

    mock_logger.error.assert_called_once()
    message_wrapper_class.return_value.add_error.assert_called_once_with(
        CreatePlateError(origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING, description=ANY)
    )
    publish_feedback.assert_called_once_with(message_wrapper_class.return_value)
    assert result is False


def test_process_when_another_exception_from_the_exporter(subject, mock_logger, mock_exporter, message_wrapper_class):
    another_exception = KeyError("key")
    mock_exporter.return_value.export_to_mongo.side_effect = another_exception
    with patch("crawler.processing.create_plate_processor.CreatePlateProcessor._publish_feedback") as publish_feedback:
        result = subject.process(MagicMock())

    mock_logger.error.assert_called_once()
    message_wrapper_class.return_value.add_error.assert_called_once_with(
        CreatePlateError(origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING, description=ANY)
    )
    publish_feedback.assert_called_once_with(message_wrapper_class.return_value)
    assert result is False


def test_process_records_the_import_when_errors_after_mongo_export(subject, mock_exporter, message_wrapper_class):
    exporter = mock_exporter.return_value

    def set_has_errors():
        message_wrapper_class.return_value.has_errors = True

    exporter.export_to_mongo.side_effect = set_has_errors

    result = subject.process(MagicMock())

    assert result is False
    exporter.record_import.assert_called_once()
    exporter.export_to_dart.assert_not_called()


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


def test_publish_feedback_publishes_valid_message(subject, create_plate_message):
    subject._publish_feedback(create_plate_message)

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
def test_publish_feedback_encodes_errors(subject, create_plate_message, mock_avro_encoder, message_errors):
    with patch.object(CreatePlateMessage, "feedback_errors", new_callable=PropertyMock) as errors_attribute:
        errors_attribute.return_value = message_errors
        subject._publish_feedback(create_plate_message)

    mock_avro_encoder.return_value.encode.assert_called_once()
    feedback_message = mock_avro_encoder.return_value.encode.call_args.args[0][0]
    assert feedback_message["operationWasErrorFree"] is False
    assert feedback_message["errors"] == message_errors
