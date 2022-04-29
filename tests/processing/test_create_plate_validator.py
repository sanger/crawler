from unittest.mock import ANY, patch

import pytest

from crawler.config.centres import CENTRE_DATA_SOURCE_RABBITMQ
from crawler.constants import CENTRE_KEY_LAB_ID_DEFAULT, RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE, RABBITMQ_FIELD_LAB_ID
from crawler.exceptions import TransientRabbitError
from crawler.processing.create_plate_validator import CreatePlateValidator
from tests.testing_objects import CREATE_PLATE_MESSAGE


@pytest.fixture
def mock_logger():
    with patch("crawler.processing.create_plate_validator.LOGGER") as logger:
        yield logger


@pytest.fixture
def subject(config):
    return CreatePlateValidator(CREATE_PLATE_MESSAGE, config)


def test_centres_gets_centres_config_from_mongo_once(subject):
    with patch("crawler.processing.create_plate_validator.get_centres_config") as gcc:
        subject.centres
        subject.centres
        subject.centres

    gcc.assert_called_once_with(subject._config, CENTRE_DATA_SOURCE_RABBITMQ)


def test_centres_raises_exception_for_loss_of_mongo_connectivity(subject):
    with patch("crawler.processing.create_plate_validator.get_centres_config") as gcc:
        gcc.side_effect = ConnectionError("Error")
        with pytest.raises(TransientRabbitError):
            subject.centres


@pytest.mark.parametrize("origin", ["origin_1", "origin_2"])
@pytest.mark.parametrize("description", ["description_1", "description_2"])
@pytest.mark.parametrize("sample_uuid", ["uuid_1", "uuid_2"])
@pytest.mark.parametrize("field", ["field_1", "field_2"])
def test_add_error_records_the_error(subject, mock_logger, origin, description, sample_uuid, field):
    subject._add_error(origin, description, sample_uuid, field)

    mock_logger.error.assert_called_once()
    logged_error = mock_logger.error.call_args.args[0]
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


def test_validate_does_nothing_if_message_valid(subject):
    subject._centres = [{CENTRE_KEY_LAB_ID_DEFAULT: "CPTD"}]

    with patch("crawler.processing.create_plate_validator.CreatePlateValidator._add_error") as add_error:
        subject.validate()

    add_error.assert_not_called()


def test_validate_adds_error_when_lab_id_not_enabled(subject):
    subject._centres = [{CENTRE_KEY_LAB_ID_DEFAULT: "CAMB"}]

    with patch("crawler.processing.create_plate_validator.CreatePlateValidator._add_error") as add_error:
        subject.validate()

    add_error.assert_called_once_with(RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE, ANY, field=RABBITMQ_FIELD_LAB_ID)
