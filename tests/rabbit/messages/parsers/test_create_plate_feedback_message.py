from unittest.mock import patch

import pytest

from crawler.rabbit.messages.parsers.create_plate_feedback_message import (
    FIELD_COUNT_OF_TOTAL_SAMPLES,
    FIELD_COUNT_OF_VALID_SAMPLES,
    FIELD_ERROR_DESCRIPTION,
    FIELD_ERROR_FIELD_NAME,
    FIELD_ERROR_ORIGIN,
    FIELD_ERROR_SAMPLE_UUID,
    FIELD_ERROR_TYPE_ID,
    FIELD_ERRORS_LIST,
    FIELD_OPERATION_WAS_ERROR_FREE,
    FIELD_SOURCE_MESSAGE_UUID,
    CreatePlateFeedbackError,
    CreatePlateFeedbackMessage,
)
from tests.testing_objects import CREATE_PLATE_FEEDBACK_MESSAGE


@pytest.fixture(autouse=True)
def logger():
    with patch("crawler.rabbit.messages.parsers.create_plate_feedback_message.LOGGER") as logger:
        yield logger


@pytest.fixture
def subject():
    return CreatePlateFeedbackMessage(CREATE_PLATE_FEEDBACK_MESSAGE)


def test_source_message_uuid_gives_expected_value(subject):
    assert subject.source_message_uuid.name == FIELD_SOURCE_MESSAGE_UUID
    assert subject.source_message_uuid.value == "SOURCE_MESSAGE_UUID"


def test_count_of_total_samples_gives_expected_value(subject):
    assert subject.count_of_total_samples.name == FIELD_COUNT_OF_TOTAL_SAMPLES
    assert subject.count_of_total_samples.value == 96


def test_count_of_valid_samples_gives_expected_value(subject):
    assert subject.count_of_valid_samples.name == FIELD_COUNT_OF_VALID_SAMPLES
    assert subject.count_of_valid_samples.value == 94


def test_operation_was_error_free_gives_expected_value(subject):
    assert subject.operation_was_error_free.name == FIELD_OPERATION_WAS_ERROR_FREE
    assert subject.operation_was_error_free.value is False


def test_errors_list_gives_list_of_appropriate_objects(subject):
    assert subject.errors_list.name == FIELD_ERRORS_LIST
    assert len(subject.errors_list.value) == 2
    assert all([type(s) == CreatePlateFeedbackError for s in subject.errors_list.value])


def test_error_type_id_gives_expected_values(subject):
    assert subject.errors_list.value[0].type_id.name == FIELD_ERROR_TYPE_ID

    type_ids = [error.type_id.value for error in subject.errors_list.value]
    assert type_ids == [1, 2]


def test_error_origin_gives_expected_values(subject):
    assert subject.errors_list.value[0].origin.name == FIELD_ERROR_ORIGIN

    origins = [error.origin.value for error in subject.errors_list.value]
    assert origins == ["Origin 1", "Origin 2"]


def test_error_sample_uuid_gives_expected_values(subject):
    assert subject.errors_list.value[0].sample_uuid.name == FIELD_ERROR_SAMPLE_UUID

    sample_uuids = [error.sample_uuid.value for error in subject.errors_list.value]
    assert sample_uuids == ["SAMPLE_1_UUID", None]


def test_error_field_name_gives_expected_values(subject):
    assert subject.errors_list.value[0].field_name.name == FIELD_ERROR_FIELD_NAME

    field_names = [error.field_name.value for error in subject.errors_list.value]
    assert field_names == ["Field 1", None]


def test_error_description_gives_expected_values(subject):
    assert subject.errors_list.value[0].description.name == FIELD_ERROR_DESCRIPTION

    descriptions = [error.description.value for error in subject.errors_list.value]
    assert descriptions == ["Description 1", "Description 2"]
