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
