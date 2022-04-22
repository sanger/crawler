from typing import NamedTuple
from unittest.mock import ANY, MagicMock, PropertyMock, patch

import pytest

from crawler.config.centres import CENTRE_DATA_SOURCE_RABBITMQ
from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import (
    CENTRE_KEY_LAB_ID_DEFAULT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
    RABBITMQ_FIELD_LAB_ID,
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
def message():
    message = MagicMock()
    type(message).message = PropertyMock(return_value=CREATE_PLATE_MESSAGE)
    type(message).errors = PropertyMock(return_value=[])

    return message


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


def test_process_calls_validate_for_message(subject, message):
    with patch("crawler.processing.create_plate_processor.CreatePlateProcessor._validate_message") as validate_message:
        subject.process(message)

    validate_message.assert_called_once_with(message)


def test_process_when_no_issues_found(subject, message):
    with patch("crawler.processing.create_plate_processor.CreatePlateProcessor._validate_message"):
        with patch(
            "crawler.processing.create_plate_processor.CreatePlateProcessor._publish_feedback"
        ) as publish_feedback:
            result = subject.process(message)

    publish_feedback.assert_called_once_with(message)
    assert result is True


def test_process_when_transient_error(subject, message, mock_logger):
    transient_error = TransientRabbitError("Test transient error")
    with patch("crawler.processing.create_plate_processor.CreatePlateProcessor._validate_message") as validate_message:
        validate_message.side_effect = transient_error

        with pytest.raises(TransientRabbitError) as ex_info:
            subject.process(message)

    mock_logger.error.assert_called_once()
    assert ex_info.value == transient_error


def test_process_when_another_exception(subject, message, mock_logger):
    another_exception = KeyError("key")
    with patch("crawler.processing.create_plate_processor.CreatePlateProcessor._validate_message") as validate_message:
        validate_message.side_effect = another_exception
        with patch(
            "crawler.processing.create_plate_processor.CreatePlateProcessor._publish_feedback"
        ) as publish_feedback:
            result = subject.process(message)

    mock_logger.error.assert_called_once()
    publish_feedback.assert_called_once_with(message, additional_errors=[ANY])
    additional_error = publish_feedback.call_args.kwargs["additional_errors"][0]
    assert additional_error["origin"] == RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING
    assert "unhandled error" in additional_error["description"].lower()
    assert result is False


def test_centres_gets_centres_config_from_mongo_once(subject):
    with patch("crawler.processing.create_plate_processor.get_centres_config") as gcc:
        subject.centres
        subject.centres
        subject.centres

    gcc.assert_called_once_with(subject._config, CENTRE_DATA_SOURCE_RABBITMQ)


def test_centres_raises_exception_for_loss_of_mongo_connectivity(subject):
    with patch("crawler.processing.create_plate_processor.get_centres_config") as gcc:
        gcc.side_effect = ConnectionError("Error")
        with pytest.raises(TransientRabbitError):
            subject.centres


def test_publish_feedback_encodes_valid_message(subject, message, mock_avro_encoder):
    subject._publish_feedback(message)

    mock_avro_encoder.return_value.encode.assert_called_once()
    feedback_message = mock_avro_encoder.return_value.encode.call_args.args[0][0]
    assert feedback_message["sourceMessageUuid"] == "b01aa0ad-7b19-4f94-87e9-70d74fb8783c"
    assert feedback_message["countOfTotalSamples"] == 0
    assert feedback_message["countOfValidSamples"] == 0
    assert feedback_message["operationWasErrorFree"] is True
    assert feedback_message["errors"] == []


def test_publish_feedback_publishes_valid_message(subject, message):
    subject._publish_feedback(message)

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
def test_publish_feedback_encodes_errors(subject, message, mock_avro_encoder, message_errors, additional_errors):
    type(message).errors = PropertyMock(return_value=message_errors)

    subject._publish_feedback(message, additional_errors)

    mock_avro_encoder.return_value.encode.assert_called_once()
    feedback_message = mock_avro_encoder.return_value.encode.call_args.args[0][0]
    assert feedback_message["operationWasErrorFree"] is False
    assert feedback_message["errors"] == message_errors + additional_errors


@pytest.mark.parametrize("origin", ["origin_1", "origin_2"])
@pytest.mark.parametrize("description", ["description_1", "description_2"])
@pytest.mark.parametrize("sample_uuid", ["uuid_1", "uuid_2"])
@pytest.mark.parametrize("field", ["field_1", "field_2"])
def test_add_error_adds_the_error_to_the_message(mock_logger, message, origin, description, sample_uuid, field):
    CreatePlateProcessor._add_error(message, origin, description, sample_uuid, field)

    mock_logger.error.assert_called_once()
    logged_error = mock_logger.error.call_args.args[0]
    assert origin in logged_error
    assert description in logged_error
    assert sample_uuid in logged_error
    assert field in logged_error

    message.add_error.assert_called_once()
    added_error = message.add_error.call_args.args[0]
    assert added_error["origin"] == origin
    assert added_error["description"] == description
    assert added_error["sampleUuid"] == sample_uuid
    assert added_error["field"] == field


def test_validate_message_does_nothing_if_message_valid(subject, message):
    subject._centres = [{CENTRE_KEY_LAB_ID_DEFAULT: "CPTD"}]

    with patch("crawler.processing.create_plate_processor.CreatePlateProcessor._add_error") as add_error:
        subject._validate_message(message)

    add_error.assert_not_called()


def test_validate_message_adds_error_when_lab_id_not_enabled(subject, message):
    subject._centres = [{CENTRE_KEY_LAB_ID_DEFAULT: "CAMB"}]

    with patch("crawler.processing.create_plate_processor.CreatePlateProcessor._add_error") as add_error:
        subject._validate_message(message)

    add_error.assert_called_once_with(message, RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE, ANY, field=RABBITMQ_FIELD_LAB_ID)
