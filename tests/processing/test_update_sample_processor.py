import copy
from typing import NamedTuple
from unittest.mock import MagicMock, patch

import pytest

from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import RABBITMQ_ROUTING_KEY_UPDATE_SAMPLE_FEEDBACK, RABBITMQ_SUBJECT_UPDATE_SAMPLE_FEEDBACK
from crawler.processing.update_sample_processor import UpdateSampleProcessor
from crawler.rabbit.messages.update_feedback_message import UpdateFeedbackMessage
from crawler.rabbit.messages.update_sample_message import MessageField, UpdateSampleMessage
from tests.testing_objects import UPDATE_SAMPLE_MESSAGE


class EncodedMessage(NamedTuple):
    body: bytes
    version: str


ENCODED_MESSAGE = EncodedMessage(body=b'{"key": "value"}', version="1")


@pytest.fixture
def logger():
    with patch("crawler.processing.update_sample_processor.LOGGER") as logger:
        yield logger


@pytest.fixture
def avro_encoder():
    with patch("crawler.processing.update_sample_processor.AvroEncoder") as avro_encoder:
        avro_encoder.return_value.encode.return_value = ENCODED_MESSAGE
        yield avro_encoder


@pytest.fixture
def update_sample_message():
    return UpdateSampleMessage(copy.deepcopy(UPDATE_SAMPLE_MESSAGE))


@pytest.fixture
def message_wrapper_class():
    with patch("crawler.processing.update_sample_processor.UpdateSampleMessage") as message_wrapper_class:
        message_wrapper_class.return_value.message_uuid = MessageField("UUID_FIELD", "UUID")
        message_wrapper_class.return_value.has_errors = False

        def add_error(error):
            message_wrapper_class.return_value.has_errors = True

        message_wrapper_class.return_value.add_error.side_effect = add_error

        yield message_wrapper_class


@pytest.fixture
def subject(config, avro_encoder):
    return UpdateSampleProcessor(MagicMock(), MagicMock(), config)


def assert_feedback_was_published(subject, message, avro_encoder):
    feedback_message = UpdateFeedbackMessage(
        sourceMessageUuid=message.message_uuid.value,
        operationWasErrorFree=not message.has_errors,
        errors=message.feedback_errors,
    )

    avro_encoder.encode.assert_called_once()
    assert avro_encoder.encode.call_args.args[0][0] == feedback_message

    subject._basic_publisher.publish_message.assert_called_once_with(
        RABBITMQ_FEEDBACK_EXCHANGE,
        RABBITMQ_ROUTING_KEY_UPDATE_SAMPLE_FEEDBACK,
        ENCODED_MESSAGE.body,
        RABBITMQ_SUBJECT_UPDATE_SAMPLE_FEEDBACK,
        ENCODED_MESSAGE.version,
    )


def test_constructor_creates_appropriate_encoder(avro_encoder):
    schema_registry = MagicMock()
    UpdateSampleProcessor(schema_registry, MagicMock(), MagicMock())

    avro_encoder.assert_called_once_with(schema_registry, RABBITMQ_SUBJECT_UPDATE_SAMPLE_FEEDBACK)


def test_process_publishes_feedback_when_no_issues_found(subject, message_wrapper_class, avro_encoder):
    subject.process(MagicMock())

    assert_feedback_was_published(subject, message_wrapper_class.return_value, avro_encoder.return_value)


def test_process_returns_true_when_no_issues_found(subject):
    result = subject.process(MagicMock())

    assert result is True
