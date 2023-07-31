from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from crawler.rabbit.messages.parsers.create_plate_message import FIELD_SAMPLE_UUID
from crawler.rabbit.messages.parsers.update_sample_message import (
    FIELD_MESSAGE_CREATE_DATE,
    FIELD_MESSAGE_UUID,
    ErrorType,
    UpdateSampleError,
    UpdateSampleMessage,
)
from tests.testing_objects import UPDATE_SAMPLE_MESSAGE


@pytest.fixture
def logger():
    with patch("crawler.rabbit.messages.parsers.update_sample_message.LOGGER") as logger:
        yield logger


@pytest.fixture
def subject():
    return UpdateSampleMessage(UPDATE_SAMPLE_MESSAGE)


def test_has_errors_is_initially_false(subject):
    assert subject.has_errors is False


def test_has_errors_is_true_after_feedback_error_logged(subject):
    subject._feedback_errors.append(MagicMock())
    assert subject.has_errors is True


def test_message_uuid_gives_expected_value(subject):
    assert subject.message_uuid.name == FIELD_MESSAGE_UUID
    assert subject.message_uuid.value == "UPDATE_SAMPLE_MESSAGE_UUID"


def test_message_create_date_gives_expected_value(subject):
    assert subject.message_create_date.name == FIELD_MESSAGE_CREATE_DATE
    assert type(subject.message_create_date.value) is datetime


def test_sample_uuid_gives_expected_value(subject):
    assert subject.sample_uuid.name == FIELD_SAMPLE_UUID
    assert subject.sample_uuid.value == "UPDATE_SAMPLE_UUID"


def test_updated_fields_gives_expected_values(subject):
    assert len(subject.updated_fields) == 2

    msField = next(x for x in subject.updated_fields.value if x.name == "mustSequence")
    assert msField.value is True

    psField = next(x for x in subject.updated_fields.value if x.name == "preferentiallySequence")
    assert psField.value is False


@pytest.mark.parametrize("description", ["description_1", "description_2"])
def test_add_error_logs_the_error_description(subject, logger, description):
    subject.add_error(
        UpdateSampleError(
            type=ErrorType.UnhandledProcessingError,
            origin="origin",
            description=description,
        )
    )

    logger.error.assert_called_once()
    logged_error = logger.error.call_args.args[0]
    assert description in logged_error


@pytest.mark.parametrize("description", ["description_1", "description_2"])
def test_add_error_records_the_textual_error(subject, description):
    subject.add_error(
        UpdateSampleError(
            type=ErrorType.UnhandledProcessingError,
            origin="origin",
            description=description,
        )
    )

    assert len(subject._textual_errors) == 1
    added_error = subject._textual_errors[0]
    assert added_error == description


@pytest.mark.parametrize("type", [ErrorType.UnhandledProcessingError])
@pytest.mark.parametrize("origin", ["origin_1", "origin_2"])
@pytest.mark.parametrize("description", ["description_1", "description_2"])
@pytest.mark.parametrize("field", ["field_1", "field_2"])
def test_add_error_records_the_feedback_error(subject, type, origin, description, field):
    subject.add_error(
        UpdateSampleError(
            type=type,
            origin=origin,
            description=description,
            field=field,
        )
    )

    assert len(subject.feedback_errors) == 1
    added_error = subject.feedback_errors[0]
    assert added_error["typeId"] == int(type)
    assert added_error["origin"] == origin
    assert added_error["description"] == description
    assert added_error["field"] == field


def test_feedback_errors_list_is_immutable(subject):
    subject.add_error(
        UpdateSampleError(type=ErrorType.UnhandledProcessingError, origin="origin", description="description")
    )

    errors = subject.feedback_errors
    assert len(errors) == 1
    errors.remove(errors[0])
    assert len(errors) == 0
    assert len(subject.feedback_errors) == 1  # Hasn't been modified
