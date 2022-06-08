import copy
from unittest.mock import ANY, patch

import pytest

from crawler.constants import RABBITMQ_UPDATE_FEEDBACK_ORIGIN_FIELD
from crawler.processing.update_sample_validator import UpdateSampleValidator
from crawler.rabbit.messages.update_sample_message import (
    FIELD_NAME,
    FIELD_SAMPLE,
    FIELD_UPDATED_FIELDS,
    FIELD_VALUE,
    ErrorType,
    UpdateSampleError,
    UpdateSampleMessage,
)
from tests.testing_objects import UPDATE_SAMPLE_MESSAGE


@pytest.fixture
def update_message():
    copy_of_message = copy.deepcopy(UPDATE_SAMPLE_MESSAGE)
    return UpdateSampleMessage(copy_of_message)


@pytest.fixture
def add_error():
    with patch.object(UpdateSampleMessage, "add_error") as add_error:
        yield add_error


@pytest.fixture
def subject(update_message):
    return UpdateSampleValidator(update_message)


def test_validate_generates_no_errors_for_valid_message(subject, update_message):
    subject.validate()

    assert update_message.has_errors is False


@pytest.mark.parametrize(
    "field_names, duped_name",
    [
        [["preferentiallySequence", "mustSequence", "preferentiallySequence"], "preferentiallySequence"],
        [["mustSequence", "preferentiallySequence", "mustSequence"], "mustSequence"],
    ],
)
def test_validate_adds_error_when_duplicate_fields_present(subject, update_message, add_error, field_names, duped_name):
    update_message._body[FIELD_SAMPLE][FIELD_UPDATED_FIELDS] = [
        {FIELD_NAME: name, FIELD_VALUE: True} for name in field_names
    ]

    subject.validate()

    add_error.assert_called_once_with(
        UpdateSampleError(
            type=ErrorType.ValidationNonUniqueFieldName,
            origin=RABBITMQ_UPDATE_FEEDBACK_ORIGIN_FIELD,
            description=ANY,
            field=FIELD_UPDATED_FIELDS,
        )
    )

    assert duped_name in add_error.call_args.args[0].description
