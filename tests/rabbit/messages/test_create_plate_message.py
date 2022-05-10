from datetime import datetime
from unittest.mock import patch

import pytest

from crawler.rabbit.messages.create_plate_message import CreatePlateMessage
from tests.testing_objects import CREATE_PLATE_MESSAGE


@pytest.fixture
def logger():
    with patch("crawler.rabbit.messages.create_plate_message.LOGGER") as logger:
        yield logger


@pytest.fixture
def subject():
    return CreatePlateMessage(CREATE_PLATE_MESSAGE)


def test_validated_samples_is_initially_zero(subject):
    assert subject.validated_samples == 0


def test_total_samples_gives_expected_value(subject):
    assert subject.total_samples == 3


def test_message_uuid_gives_expected_value(subject):
    assert subject.message_uuid.name == "messageUuid"
    assert subject.message_uuid.value == "b01aa0ad-7b19-4f94-87e9-70d74fb8783c"


def test_message_create_date_gives_expected_value(subject):
    assert subject.message_create_date.name == "messageCreateDateUtc"
    assert type(subject.message_create_date.value) == datetime


def test_plate_lab_id_gives_expected_value(subject):
    assert subject.lab_id.name == "labId"
    assert subject.lab_id.value == "CPTD"


def test_plate_barcode_gives_expected_value(subject):
    assert subject.plate_barcode.name == "plateBarcode"
    assert subject.plate_barcode.value == "PLATE-001"


@pytest.mark.parametrize("origin", ["origin_1", "origin_2"])
@pytest.mark.parametrize("description", ["description_1", "description_2"])
@pytest.mark.parametrize("sample_uuid", ["uuid_1", "uuid_2"])
@pytest.mark.parametrize("field", ["field_1", "field_2"])
def test_add_error_records_the_error(subject, logger, origin, description, sample_uuid, field):
    subject.add_error(origin, description, sample_uuid, field)

    logger.error.assert_called_once()
    logged_error = logger.error.call_args.args[0]
    assert origin in logged_error
    assert description in logged_error
    assert sample_uuid in logged_error
    assert field in logged_error

    assert len(subject.errors) == 1
    added_error = subject.errors[0]
    assert added_error["origin"] == origin
    assert added_error["description"] == description
    assert added_error["sampleUuid"] == sample_uuid
    assert added_error["field"] == field


def test_errors_list_is_immutable(subject):
    subject.add_error("origin", "description", "sample_uuid", "field")

    errors = subject.errors
    assert len(errors) == 1
    errors.remove(errors[0])
    assert len(errors) == 0
    assert len(subject.errors) == 1  # Hasn't been modified
