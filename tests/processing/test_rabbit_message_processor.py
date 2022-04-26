from unittest.mock import MagicMock, patch

import pytest

from crawler.constants import RABBITMQ_HEADER_KEY_SUBJECT, RABBITMQ_HEADER_KEY_VERSION, RABBITMQ_SUBJECT_CREATE_PLATE
from crawler.exceptions import TransientRabbitError
from crawler.processing.rabbit_message_processor import RabbitMessageProcessor

SCHEMA_REGISTRY = MagicMock()
BASIC_PUBLISHER = MagicMock()

HEADERS = {
    RABBITMQ_HEADER_KEY_SUBJECT: RABBITMQ_SUBJECT_CREATE_PLATE,
    RABBITMQ_HEADER_KEY_VERSION: "3",
}
MESSAGE_BODY = "Body"


@pytest.fixture
def logger():
    with patch("crawler.processing.rabbit_message_processor.LOGGER") as logger:
        yield logger


@pytest.fixture
def rabbit_message():
    with patch("crawler.processing.rabbit_message_processor.RabbitMessage") as rabbit_message:
        rabbit_message.return_value.subject = HEADERS[RABBITMQ_HEADER_KEY_SUBJECT]
        yield rabbit_message


@pytest.fixture
def avro_encoder():
    with patch("crawler.processing.rabbit_message_processor.AvroEncoder") as avro_encoder:
        yield avro_encoder


@pytest.fixture
def create_plate_processor():
    with patch("crawler.processing.rabbit_message_processor.CreatePlateProcessor") as create_processor:
        yield create_processor


@pytest.fixture
def subject(config, create_plate_processor, rabbit_message, avro_encoder):
    subject = RabbitMessageProcessor(SCHEMA_REGISTRY, BASIC_PUBLISHER, config)
    subject._processors = {RABBITMQ_SUBJECT_CREATE_PLATE: create_plate_processor.return_value}
    yield subject


def test_constructor_stored_passed_values(subject, config):
    assert subject._schema_registry == SCHEMA_REGISTRY
    assert subject._basic_publisher == BASIC_PUBLISHER
    assert subject._config == config


def test_constructor_populated_processors_correctly(subject, create_plate_processor):
    assert list(subject._processors.keys()) == [RABBITMQ_SUBJECT_CREATE_PLATE]
    assert subject._processors[RABBITMQ_SUBJECT_CREATE_PLATE] == create_plate_processor.return_value


def test_process_message_decodes_the_message(subject, rabbit_message, avro_encoder):
    subject.process_message(HEADERS, MESSAGE_BODY)

    rabbit_message.assert_called_once_with(HEADERS, MESSAGE_BODY)
    avro_encoder.assert_called_once_with(SCHEMA_REGISTRY, HEADERS[RABBITMQ_HEADER_KEY_SUBJECT])
    rabbit_message.return_value.decode.assert_called_once_with(avro_encoder.return_value)


def test_process_message_handles_exception_during_decode(subject, logger, rabbit_message):
    rabbit_message.return_value.decode.side_effect = KeyError()
    result = subject.process_message(HEADERS, MESSAGE_BODY)

    assert result is False
    logger.error.assert_called_once()
    error_log = logger.error.call_args.args[0]
    assert "unrecoverable" in error_log.lower()


def test_process_message_handles_transient_error_from_schema_registry(subject, logger, rabbit_message):
    # We have mocked out the decode method.  The AvroEncoder speaks to the schema registry
    # which could raise this error type so we'll just mock it on the decode method.
    error_message = "Schema registry unreachable"
    rabbit_message.return_value.decode.side_effect = TransientRabbitError(error_message)

    with pytest.raises(TransientRabbitError):
        subject.process_message(HEADERS, MESSAGE_BODY)

    logger.error.assert_called_once()
    error_log = logger.error.call_args.args[0]
    assert "transient" in error_log.lower()
    assert error_message in error_log


def test_process_message_rejects_rabbit_message_with_multiple_messages(subject, logger, rabbit_message):
    rabbit_message.return_value.contains_single_message = False
    result = subject.process_message(HEADERS, MESSAGE_BODY)

    assert result is False
    logger.error.assert_called_once()
    error_log = logger.error.call_args.args[0]
    assert "multiple" in error_log.lower()


def test_process_message_rejects_rabbit_message_with_unrecognised_subject(subject, logger, rabbit_message):
    wrong_subject = "random-subject"
    rabbit_message.return_value.subject = wrong_subject
    result = subject.process_message(HEADERS, MESSAGE_BODY)

    assert result is False
    logger.error.assert_called_once()
    error_log = logger.error.call_args.args[0]
    assert wrong_subject in error_log


@pytest.mark.parametrize("return_value", [True, False])
def test_process_message_returns_value_returned_by_processor(subject, create_plate_processor, return_value):
    create_plate_processor.return_value.process.return_value = return_value
    result = subject.process_message(HEADERS, MESSAGE_BODY)

    assert result is return_value


def test_process_message_raises_error_generated_by_processor(subject, create_plate_processor):
    raised_error = TransientRabbitError("Test")
    create_plate_processor.return_value.process.side_effect = raised_error

    with pytest.raises(TransientRabbitError) as ex_info:
        subject.process_message(HEADERS, MESSAGE_BODY)

    assert ex_info.value == raised_error
