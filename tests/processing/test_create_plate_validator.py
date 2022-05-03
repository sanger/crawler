import copy
from datetime import datetime
from unittest.mock import ANY, call, patch

import pytest

from crawler.config.centres import CENTRE_DATA_SOURCE_RABBITMQ
from crawler.constants import (
    CENTRE_KEY_LAB_ID_DEFAULT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
    RABBITMQ_FIELD_LAB_ID,
    RABBITMQ_FIELD_MESSAGE_CREATE_DATE,
    RABBITMQ_FIELD_PLATE,
    RABBITMQ_FIELD_PLATE_BARCODE,
    RABBITMQ_FIELD_PLATE_COORDINATE,
    RABBITMQ_FIELD_RNA_ID,
    RABBITMQ_FIELD_ROOT_SAMPLE_ID,
    RABBITMQ_FIELD_SAMPLE_UUID,
    RABBITMQ_FIELD_SAMPLES,
    RABBITMQ_FIELD_TESTED_DATE,
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


@pytest.mark.parametrize("plate_barcode", ["plate_barcode_1", "plate_barcode_2"])
@pytest.mark.parametrize(
    "sample_uuid", [b"37f35f76-d4cf-4ffd-9fb1-bafde824fd46", b"34d623e0-ecd9-4ffe-b6bc-a2573bb27b22"]
)
@pytest.mark.parametrize("root_sample_id", ["R00T-S4MPL3-1D-01", "R00T-S4MPL3-1D-02"])
@pytest.mark.parametrize("rna_id", ["RN4-1D-01", "RN4-1D-02"])
@pytest.mark.parametrize(
    "plate_coordinate",
    [
        "A1",
        "A01",
        "A2",
        "A02",
        "A3",
        "A03",
        "A4",
        "A04",
        "A5",
        "A05",
        "A6",
        "A06",
        "A7",
        "A07",
        "A8",
        "A08",
        "A9",
        "A09",
        "A10",
        "A11",
        "A12",
        "B1",
        "B01",
        "C1",
        "C01",
        "D1",
        "D01",
        "E1",
        "E01",
        "F1",
        "F01",
        "G1",
        "G01",
        "H1",
        "H01",
    ],
)
@pytest.mark.parametrize(
    "tested_date",
    [datetime(2022, 2, 14, 7, 24, 35), datetime(2021, 12, 31, 23, 59, 59), datetime(2022, 2, 13, 14, 30, 0)],
)
def test_validate_generates_no_errors_and_counts_samples_when_message_is_valid(
    subject, plate_barcode, sample_uuid, root_sample_id, rna_id, plate_coordinate, tested_date
):
    subject.message[RABBITMQ_FIELD_MESSAGE_CREATE_DATE] = datetime(2022, 2, 14, 7, 24, 35)
    subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_PLATE_BARCODE] = plate_barcode

    # Just keep one sample to modify values on
    sample = subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES][0]
    subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES] = [sample]
    sample[RABBITMQ_FIELD_SAMPLE_UUID] = sample_uuid
    sample[RABBITMQ_FIELD_ROOT_SAMPLE_ID] = root_sample_id
    sample[RABBITMQ_FIELD_RNA_ID] = rna_id
    sample[RABBITMQ_FIELD_PLATE_COORDINATE] = plate_coordinate
    sample[RABBITMQ_FIELD_TESTED_DATE] = tested_date

    subject.validate()

    assert subject.errors == []
    assert subject.total_samples == 1
    assert subject.valid_samples == 1


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


@pytest.mark.parametrize("invalid_column", ["", "001", "0", "00", "13", "013", "A"])
def test_validate_adds_error_when_plate_coordinate_column_invalid(subject, add_error, invalid_column):
    samples = subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES]
    samples[0][RABBITMQ_FIELD_PLATE_COORDINATE] = f"A{invalid_column}"
    samples[1][RABBITMQ_FIELD_PLATE_COORDINATE] = f"A{invalid_column}"

    subject.validate()

    # We're only expecting 2 calls.  There should not be a call indicating that the empty values are not unique.
    add_error.assert_has_calls(
        [
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value does not match regex (^[A-H](?:0?[1-9]|1[0-2])$).",
                samples[0][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_PLATE_COORDINATE,
            ),
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value does not match regex (^[A-H](?:0?[1-9]|1[0-2])$).",
                samples[1][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_PLATE_COORDINATE,
            ),
        ]
    )

    assert subject.total_samples == 3
    assert subject.valid_samples == 1


@pytest.mark.parametrize(
    "invalid_row",
    ["", "0", "01", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"],
)
def test_validate_adds_error_when_plate_coordinate_column_invalid(subject, add_error, invalid_row):
    samples = subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES]
    samples[0][RABBITMQ_FIELD_PLATE_COORDINATE] = f"{invalid_row}03"
    samples[1][RABBITMQ_FIELD_PLATE_COORDINATE] = f"{invalid_row}03"

    subject.validate()

    # We're only expecting 2 calls.  There should not be a call indicating that the empty values are not unique.
    add_error.assert_has_calls(
        [
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value does not match regex (^[A-H](?:0?[1-9]|1[0-2])$).",
                samples[0][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_PLATE_COORDINATE,
            ),
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value does not match regex (^[A-H](?:0?[1-9]|1[0-2])$).",
                samples[1][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_PLATE_COORDINATE,
            ),
        ]
    )

    assert subject.total_samples == 3
    assert subject.valid_samples == 1


def test_validate_adds_error_when_plate_coordinate_is_not_unique(subject, add_error):
    samples = subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES]
    samples[0][RABBITMQ_FIELD_PLATE_COORDINATE] = "E06"
    samples[1][RABBITMQ_FIELD_PLATE_COORDINATE] = "E06"

    subject.validate()

    add_error.assert_has_calls(
        [
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value is not unique across samples (E06).",
                samples[0][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_PLATE_COORDINATE,
            ),
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value is not unique across samples (E06).",
                samples[1][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_PLATE_COORDINATE,
            ),
        ]
    )

    assert subject.total_samples == 3
    assert subject.valid_samples == 1


def test_validate_adds_error_when_tested_date_is_too_recent(subject, add_error):
    subject.message[RABBITMQ_FIELD_MESSAGE_CREATE_DATE] = datetime(2022, 4, 29, 12, 34, 56)
    samples = subject.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES]
    samples[0][RABBITMQ_FIELD_TESTED_DATE] = datetime(2022, 4, 29, 12, 34, 57)  # one second too late
    samples[1][RABBITMQ_FIELD_TESTED_DATE] = datetime(2023, 4, 29, 12, 34, 56)  # one year too late
    samples[2][RABBITMQ_FIELD_TESTED_DATE] = datetime(2022, 4, 29, 12, 34, 56)  # this is OK

    subject.validate()

    add_error.assert_has_calls(
        [
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value repesents a timestamp that is too recent (2022-04-29 12:34:57 > 2022-04-29 12:34:56).",
                samples[0][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_TESTED_DATE,
            ),
            call(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                "Field value repesents a timestamp that is too recent (2023-04-29 12:34:56 > 2022-04-29 12:34:56).",
                samples[1][RABBITMQ_FIELD_SAMPLE_UUID].decode(),
                RABBITMQ_FIELD_TESTED_DATE,
            ),
        ]
    )

    assert subject.total_samples == 3
    assert subject.valid_samples == 1
