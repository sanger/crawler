import copy
from unittest.mock import ANY, call, patch

import pytest

from crawler.config.centres import CENTRE_DATA_SOURCE_RABBITMQ
from crawler.constants import (
    CENTRE_KEY_LAB_ID_DEFAULT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
    RABBITMQ_FIELD_LAB_ID,
    RABBITMQ_FIELD_PLATE,
    RABBITMQ_FIELD_PLATE_BARCODE,
    RABBITMQ_FIELD_RNA_ID,
    RABBITMQ_FIELD_ROOT_SAMPLE_ID,
    RABBITMQ_FIELD_SAMPLE_UUID,
    RABBITMQ_FIELD_SAMPLES,
)
from crawler.exceptions import TransientRabbitError
from crawler.processing.create_plate_validator import CreatePlateValidator
from tests.testing_objects import CREATE_PLATE_MESSAGE


@pytest.fixture
def logger():
    with patch("crawler.processing.create_plate_validator.LOGGER") as logger:
        yield logger


@pytest.fixture
def add_error():
    with patch("crawler.processing.create_plate_validator.CreatePlateValidator._add_error") as add_error:
        yield add_error


@pytest.fixture
def subject(config):
    copy_of_message = copy.deepcopy(CREATE_PLATE_MESSAGE)
    validator = CreatePlateValidator(copy_of_message, config)
    validator._centres = [{CENTRE_KEY_LAB_ID_DEFAULT: "CPTD"}]

    return validator


def test_centres_gets_centres_config_from_mongo_once(subject):
    subject._centres = None

    with patch("crawler.processing.create_plate_validator.get_centres_config") as gcc:
        subject.centres
        subject.centres
        subject.centres

    gcc.assert_called_once_with(subject._config, CENTRE_DATA_SOURCE_RABBITMQ)


def test_centres_raises_exception_for_loss_of_mongo_connectivity(subject):
    subject._centres = None

    with patch("crawler.processing.create_plate_validator.get_centres_config") as gcc:
        gcc.side_effect = ConnectionError("Error")
        with pytest.raises(TransientRabbitError):
            subject.centres


@pytest.mark.parametrize("origin", ["origin_1", "origin_2"])
@pytest.mark.parametrize("description", ["description_1", "description_2"])
@pytest.mark.parametrize("sample_uuid", ["uuid_1", "uuid_2"])
@pytest.mark.parametrize("field", ["field_1", "field_2"])
def test_add_error_records_the_error(subject, logger, origin, description, sample_uuid, field):
    subject._add_error(origin, description, sample_uuid, field)

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


def test_validate_generates_no_errors_when_message_is_valid(subject):
    subject.validate()

    assert subject.errors == []


def test_validate_counts_samples_correctly(subject):
    subject.validate()

    assert subject.total_samples == 3
    assert subject.valid_samples == 3


def test_validate_adds_error_when_lab_id_not_enabled(subject, add_error):
    subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_LAB_ID] = "NOT_A_CENTRE"

    subject.validate()

    add_error.assert_called_once_with(RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE, ANY, field=RABBITMQ_FIELD_LAB_ID)

    assert subject.total_samples == 3
    assert subject.valid_samples == 3


def test_validate_adds_error_when_plate_barcode_is_empty(subject, add_error):
    subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_PLATE_BARCODE] = ""

    subject.validate()

    add_error.assert_called_once_with(
        RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE, "Field value is not populated.", field=RABBITMQ_FIELD_PLATE_BARCODE
    )

    assert subject.total_samples == 3
    assert subject.valid_samples == 3


def test_validate_adds_single_error_when_multiple_samples_have_the_same_uuid(subject, add_error):
    sample_uuid = "01234567-89ab-cdef-0123-456789abcdef"
    for sample in subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES]:
        sample[RABBITMQ_FIELD_SAMPLE_UUID] = sample_uuid.encode()

    subject.validate()

    add_error.assert_called_once_with(
        RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
        f"Sample UUID {sample_uuid} exists more than once in the message.",
        sample_uuid=sample_uuid,
        field=RABBITMQ_FIELD_SAMPLE_UUID,
    )

    assert subject.total_samples == 3
    assert subject.valid_samples == 0


def test_validate_adds_error_when_root_sample_id_is_empty(subject, add_error):
    samples = subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES]
    samples[0][RABBITMQ_FIELD_ROOT_SAMPLE_ID] = ""
    samples[1][RABBITMQ_FIELD_ROOT_SAMPLE_ID] = ""

    subject.validate()

    # We're only expecting 2 calls.  There should not be a call indicating that the empty values are not unique.
    add_error.assert_has_calls(
        [
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value is not populated.",
                samples[0][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_ROOT_SAMPLE_ID,
            ),
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value is not populated.",
                samples[1][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_ROOT_SAMPLE_ID,
            ),
        ]
    )

    assert subject.total_samples == 3
    assert subject.valid_samples == 1


def test_validate_adds_error_when_root_sample_id_is_not_unique(subject, add_error):
    samples = subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES]
    samples[0][RABBITMQ_FIELD_ROOT_SAMPLE_ID] = "ROOT-SAMPLE-ID"
    samples[1][RABBITMQ_FIELD_ROOT_SAMPLE_ID] = "ROOT-SAMPLE-ID"

    subject.validate()

    add_error.assert_has_calls(
        [
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value is not unique across samples (ROOT-SAMPLE-ID).",
                samples[0][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_ROOT_SAMPLE_ID,
            ),
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value is not unique across samples (ROOT-SAMPLE-ID).",
                samples[1][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_ROOT_SAMPLE_ID,
            ),
        ]
    )

    assert subject.total_samples == 3
    assert subject.valid_samples == 1


def test_validate_adds_error_when_rna_id_is_empty(subject, add_error):
    samples = subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES]
    samples[0][RABBITMQ_FIELD_RNA_ID] = ""
    samples[1][RABBITMQ_FIELD_RNA_ID] = ""

    subject.validate()

    # We're only expecting 2 calls.  There should not be a call indicating that the empty values are not unique.
    add_error.assert_has_calls(
        [
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value is not populated.",
                samples[0][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_RNA_ID,
            ),
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value is not populated.",
                samples[1][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_RNA_ID,
            ),
        ]
    )

    assert subject.total_samples == 3
    assert subject.valid_samples == 1


def test_validate_adds_error_when_rna_id_is_not_unique(subject, add_error):
    samples = subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES]
    samples[0][RABBITMQ_FIELD_RNA_ID] = "RNA-ID"
    samples[1][RABBITMQ_FIELD_RNA_ID] = "RNA-ID"

    subject.validate()

    add_error.assert_has_calls(
        [
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value is not unique across samples (RNA-ID).",
                samples[0][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_RNA_ID,
            ),
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value is not unique across samples (RNA-ID).",
                samples[1][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_RNA_ID,
            ),
        ]
    )

    assert subject.total_samples == 3
    assert subject.valid_samples == 1
