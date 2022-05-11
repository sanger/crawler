from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pytest

from crawler.rabbit.messages.create_plate_message import (
    FIELD_COG_UK_ID,
    FIELD_FIT_TO_PICK,
    FIELD_LAB_ID,
    FIELD_MESSAGE_CREATE_DATE,
    FIELD_MESSAGE_UUID,
    FIELD_MUST_SEQUENCE,
    FIELD_PLATE,
    FIELD_PLATE_BARCODE,
    FIELD_PLATE_COORDINATE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SAMPLE_UUID,
    FIELD_SAMPLES,
    FIELD_TESTED_DATE,
    CreatePlateError,
    CreatePlateMessage,
    CreatePlateSample,
)
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


def test_has_errors_is_initially_false(subject):
    assert subject.has_errors is False


def test_has_errors_is_true_after_feedback_error_logged(subject):
    subject._feedback_errors.append(MagicMock())
    assert subject.has_errors is True


def test_has_errors_is_true_after_textual_error_logged(subject):
    subject._textual_errors.append(MagicMock())
    assert subject.has_errors is True


def test_total_samples_gives_expected_value(subject):
    assert subject.total_samples == 3


def test_message_uuid_gives_expected_value(subject):
    assert subject.message_uuid.name == FIELD_MESSAGE_UUID
    assert subject.message_uuid.value == "b01aa0ad-7b19-4f94-87e9-70d74fb8783c"


def test_message_create_date_gives_expected_value(subject):
    assert subject.message_create_date.name == FIELD_MESSAGE_CREATE_DATE
    assert type(subject.message_create_date.value) == datetime


def test_plate_lab_id_gives_expected_value(subject):
    assert subject.lab_id.name == FIELD_LAB_ID
    assert subject.lab_id.value == "CPTD"


def test_plate_barcode_gives_expected_value(subject):
    assert subject.plate_barcode.name == FIELD_PLATE_BARCODE
    assert subject.plate_barcode.value == "PLATE-001"


def test_samples_gives_list_of_appropriate_objects(subject):
    assert subject.samples.name == FIELD_SAMPLES
    assert len(subject.samples.value) == 3
    assert all([type(s) == CreatePlateSample for s in subject.samples.value])


def test_sample_cog_uk_id_gives_expected_value(subject):
    sample = subject.samples.value[0]
    assert sample.cog_uk_id.name == FIELD_COG_UK_ID
    assert sample.cog_uk_id.value == "C0G-UK-ID-1"


def test_sample_fit_to_pick_gives_expected_value(subject):
    sample = subject.samples.value[0]
    assert sample.fit_to_pick.name == FIELD_FIT_TO_PICK
    assert sample.fit_to_pick.value is True


def test_sample_must_sequence_gives_expected_value(subject):
    sample = subject.samples.value[0]
    assert sample.must_sequence.name == FIELD_MUST_SEQUENCE
    assert sample.must_sequence.value is False


def test_sample_plate_coordinate_gives_expected_value(subject):
    sample = subject.samples.value[0]
    assert sample.plate_coordinate.name == FIELD_PLATE_COORDINATE
    assert sample.plate_coordinate.value == "A1"


def test_sample_preferentially_sequence_gives_expected_value(subject):
    sample = subject.samples.value[0]
    assert sample.preferentially_sequence.name == FIELD_PREFERENTIALLY_SEQUENCE
    assert sample.preferentially_sequence.value is False


def test_sample_result_gives_expected_value(subject):
    sample = subject.samples.value[0]
    assert sample.result.name == FIELD_RESULT
    assert sample.result.value == "positive"


def test_sample_rna_id_gives_expected_value(subject):
    sample = subject.samples.value[0]
    assert sample.rna_id.name == FIELD_RNA_ID
    assert sample.rna_id.value == "RN4-1D-1"


def test_sample_root_sample_id_gives_expected_value(subject):
    sample = subject.samples.value[0]
    assert sample.root_sample_id.name == FIELD_ROOT_SAMPLE_ID
    assert sample.root_sample_id.value == "R00T-S4MPL3-ID1"


def test_sample_uuid_gives_expected_value(subject):
    sample = subject.samples.value[0]
    assert sample.sample_uuid.name == FIELD_SAMPLE_UUID
    assert sample.sample_uuid.value == "0aae6004-8e01-4f7a-9d50-91c51052813f"


def test_sample_tested_date_gives_expected_value(subject):
    sample = subject.samples.value[0]
    assert sample.tested_date.name == FIELD_TESTED_DATE
    assert sample.tested_date.value == datetime(2022, 4, 10, 11, 45, 25)


def test_duplicated_sample_values_gives_no_duplicates_for_good_message(subject):
    assert subject.duplicated_sample_values == {
        FIELD_SAMPLE_UUID: set(),
        FIELD_ROOT_SAMPLE_ID: set(),
        FIELD_RNA_ID: set(),
        FIELD_COG_UK_ID: set(),
        FIELD_PLATE_COORDINATE: set(),
    }


MESSAGE_WITH_DUPLICATES = {
    FIELD_PLATE: {
        FIELD_SAMPLES: [
            {
                FIELD_SAMPLE_UUID: b"0aae6004-8e01-4f7a-9d50-91c51052813f",
                FIELD_ROOT_SAMPLE_ID: "R00T-S4MPL3-ID1",
                FIELD_RNA_ID: "RN4-1D-1",
                FIELD_COG_UK_ID: "C0G-UK-ID-1",
                FIELD_PLATE_COORDINATE: "A1",
            },
            {
                FIELD_SAMPLE_UUID: b"0aae6004-8e01-4f7a-9d50-91c51052813f",
                FIELD_ROOT_SAMPLE_ID: "R00T-S4MPL3-ID1",
                FIELD_RNA_ID: "RN4-1D-1",
                FIELD_COG_UK_ID: "C0G-UK-ID-1",
                FIELD_PLATE_COORDINATE: "A01",
            },
        ]
    }
}


def test_duplicated_sample_values_gives_correct_duplicates_for_bad_message():
    subject = CreatePlateMessage(MESSAGE_WITH_DUPLICATES)

    assert subject.duplicated_sample_values == {
        FIELD_SAMPLE_UUID: set(["0aae6004-8e01-4f7a-9d50-91c51052813f"]),
        FIELD_ROOT_SAMPLE_ID: set(["R00T-S4MPL3-ID1"]),
        FIELD_RNA_ID: set(["RN4-1D-1"]),
        FIELD_COG_UK_ID: set(["C0G-UK-ID-1"]),
        FIELD_PLATE_COORDINATE: set(["A01"]),
    }


def test_duplicated_sample_values_calls_expected_methods():
    subject = CreatePlateMessage(MESSAGE_WITH_DUPLICATES)

    with patch("crawler.rabbit.messages.create_plate_message.extract_dupes") as extract_dupes:
        with patch("crawler.rabbit.messages.create_plate_message.normalise_plate_coordinate") as normalise:
            normalise.return_value = "A01"
            subject.duplicated_sample_values

    extract_dupes.assert_has_calls(
        [
            call(["0aae6004-8e01-4f7a-9d50-91c51052813f", "0aae6004-8e01-4f7a-9d50-91c51052813f"]),
            call(["R00T-S4MPL3-ID1", "R00T-S4MPL3-ID1"]),
            call(["RN4-1D-1", "RN4-1D-1"]),
            call(["C0G-UK-ID-1", "C0G-UK-ID-1"]),
            call(["A01", "A01"]),
        ]
    )

    normalise.assert_has_calls([call("A1"), call("A01")])


@pytest.mark.parametrize("description", ["description_1", "description_2"])
@pytest.mark.parametrize("long_description", [None, "long_desc_1", "long_desc_2"])
def test_add_error_logs_the_error_description(subject, logger, description, long_description):
    subject.add_error(
        CreatePlateError(
            origin="origin",
            description=description,
            long_description=long_description,
        )
    )

    logger.error.assert_called_once()
    logged_error = logger.error.call_args.args[0]

    if long_description is None:
        assert description in logged_error
    else:
        assert long_description in logged_error


@pytest.mark.parametrize("description", ["description_1", "description_2"])
@pytest.mark.parametrize("long_description", [None, "long_desc_1", "long_desc_2"])
def test_add_error_records_the_textual_error(subject, description, long_description):
    subject.add_error(
        CreatePlateError(
            origin="origin",
            description=description,
            long_description=long_description,
        )
    )

    assert len(subject.textual_errors) == 1
    added_error = subject.textual_errors[0]

    if long_description is None:
        assert added_error == description
    else:
        assert added_error == long_description


def test_textual_errors_list_is_immutable(subject):
    subject.add_error(CreatePlateError(origin="origin", description="description"))

    errors = subject.textual_errors
    assert len(errors) == 1
    errors.remove(errors[0])
    assert len(errors) == 0
    assert len(subject.textual_errors) == 1  # Hasn't been modified


@pytest.mark.parametrize("origin", ["origin_1", "origin_2"])
@pytest.mark.parametrize("description", ["description_1", "description_2"])
@pytest.mark.parametrize("sample_uuid", ["uuid_1", "uuid_2"])
@pytest.mark.parametrize("field", ["field_1", "field_2"])
@pytest.mark.parametrize("long_description", [None, "long_desc_1", "long_desc_2"])
def test_add_error_records_the_feedback_error(subject, origin, description, sample_uuid, field, long_description):
    subject.add_error(
        CreatePlateError(
            origin=origin,
            description=description,
            sample_uuid=sample_uuid,
            field=field,
            long_description=long_description,
        )
    )

    assert len(subject.feedback_errors) == 1
    added_error = subject.feedback_errors[0]
    assert added_error["origin"] == origin
    assert added_error["description"] == description
    assert added_error["sampleUuid"] == sample_uuid
    assert added_error["field"] == field


def test_feedback_errors_list_is_immutable(subject):
    subject.add_error(CreatePlateError(origin="origin", description="description"))

    errors = subject.feedback_errors
    assert len(errors) == 1
    errors.remove(errors[0])
    assert len(errors) == 0
    assert len(subject.feedback_errors) == 1  # Hasn't been modified
