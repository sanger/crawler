import copy
from datetime import datetime
from unittest.mock import ANY, call, patch

import pytest

from crawler.config.centres import CENTRE_DATA_SOURCE_RABBITMQ
from crawler.constants import (
    CENTRE_KEY_LAB_ID_DEFAULT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
)
from crawler.exceptions import TransientRabbitError
from crawler.processing.create_plate_validator import CreatePlateValidator
from crawler.rabbit.messages.create_plate_message import (
    FIELD_COG_UK_ID,
    FIELD_LAB_ID,
    FIELD_MESSAGE_CREATE_DATE,
    FIELD_PLATE,
    FIELD_PLATE_BARCODE,
    FIELD_PLATE_COORDINATE,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SAMPLE_UUID,
    FIELD_SAMPLES,
    FIELD_TESTED_DATE,
    CreatePlateError,
    CreatePlateMessage,
)
from tests.testing_objects import CREATE_PLATE_MESSAGE


@pytest.fixture
def create_message():
    copy_of_message = copy.deepcopy(CREATE_PLATE_MESSAGE)
    return CreatePlateMessage(copy_of_message)


@pytest.fixture
def add_error():
    with patch.object(CreatePlateMessage, "add_error") as add_error:
        yield add_error


@pytest.fixture
def subject(create_message, config):
    validator = CreatePlateValidator(create_message, config)
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


DEFAULT_TESTED_DATE = datetime(2021, 1, 1, 0, 0, 0)


def assert_validate_when_message_is_valid(
    subject,
    create_message,
    plate_barcode="PLATE_BARCODE",
    sample_uuid=b"SAMPLE_UUID",
    root_sample_id="ROOT_SAMPLE_ID",
    rna_id="RNA_ID",
    cog_uk_id="COG_UK_ID",
    plate_coordinate="A1",
    tested_date=DEFAULT_TESTED_DATE,
):
    create_message._body[FIELD_MESSAGE_CREATE_DATE] = datetime(2022, 2, 14, 7, 24, 35)
    create_message._body[FIELD_PLATE][FIELD_PLATE_BARCODE] = plate_barcode

    # Just keep one sample to modify values on
    sample = create_message._body[FIELD_PLATE][FIELD_SAMPLES][0]
    create_message._body[FIELD_PLATE][FIELD_SAMPLES] = [sample]
    sample[FIELD_SAMPLE_UUID] = sample_uuid
    sample[FIELD_ROOT_SAMPLE_ID] = root_sample_id
    sample[FIELD_RNA_ID] = rna_id
    sample[FIELD_COG_UK_ID] = cog_uk_id
    sample[FIELD_PLATE_COORDINATE] = plate_coordinate
    sample[FIELD_TESTED_DATE] = tested_date

    subject.validate()

    assert create_message.errors == []
    assert create_message.total_samples == 1
    assert create_message.validated_samples == 1


@pytest.mark.parametrize("plate_barcode", ["plate_barcode_1", "plate_barcode_2"])
def test_validate_generates_no_errors_for_valid_plate_barcodes(subject, create_message, plate_barcode):
    assert_validate_when_message_is_valid(subject, create_message, plate_barcode=plate_barcode)


@pytest.mark.parametrize(
    "sample_uuid", [b"37f35f76-d4cf-4ffd-9fb1-bafde824fd46", b"34d623e0-ecd9-4ffe-b6bc-a2573bb27b22"]
)
def test_validate_generates_no_errors_for_valid_sample_uuids(subject, create_message, sample_uuid):
    assert_validate_when_message_is_valid(subject, create_message, sample_uuid=sample_uuid)


@pytest.mark.parametrize("root_sample_id", ["R00T-S4MPL3-1D-01", "R00T-S4MPL3-1D-02"])
def test_validate_generates_no_errors_for_valid_root_sample_ids(subject, create_message, root_sample_id):
    assert_validate_when_message_is_valid(subject, create_message, root_sample_id=root_sample_id)


@pytest.mark.parametrize("rna_id", ["RN4-1D-01", "RN4-1D-02"])
def test_validate_generates_no_errors_for_valid_rna_ids(subject, create_message, rna_id):
    assert_validate_when_message_is_valid(subject, create_message, rna_id=rna_id)


@pytest.mark.parametrize("cog_uk_id", ["COG-UK-ID-1", "COG-UK-ID-2"])
def test_validate_generates_no_errors_for_valid_cog_uk_ids(subject, create_message, cog_uk_id):
    assert_validate_when_message_is_valid(subject, create_message, cog_uk_id=cog_uk_id)


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
        "B2",
        "B02",
        "B3",
        "B03",
        "B4",
        "B04",
        "B5",
        "B05",
        "B6",
        "B06",
        "B7",
        "B07",
        "B8",
        "B08",
        "B9",
        "B09",
        "B10",
        "B11",
        "B12",
        "C1",
        "C01",
        "C2",
        "C02",
        "C3",
        "C03",
        "C4",
        "C04",
        "C5",
        "C05",
        "C6",
        "C06",
        "C7",
        "C07",
        "C8",
        "C08",
        "C9",
        "C09",
        "C10",
        "C11",
        "C12",
        "D1",
        "D01",
        "D2",
        "D02",
        "D3",
        "D03",
        "D4",
        "D04",
        "D5",
        "D05",
        "D6",
        "D06",
        "D7",
        "D07",
        "D8",
        "D08",
        "D9",
        "D09",
        "D10",
        "D11",
        "D12",
        "E1",
        "E01",
        "E2",
        "E02",
        "E3",
        "E03",
        "E4",
        "E04",
        "E5",
        "E05",
        "E6",
        "E06",
        "E7",
        "E07",
        "E8",
        "E08",
        "E9",
        "E09",
        "E10",
        "E11",
        "E12",
        "F1",
        "F01",
        "F2",
        "F02",
        "F3",
        "F03",
        "F4",
        "F04",
        "F5",
        "F05",
        "F6",
        "F06",
        "F7",
        "F07",
        "F8",
        "F08",
        "F9",
        "F09",
        "F10",
        "F11",
        "F12",
        "G1",
        "G01",
        "G2",
        "G02",
        "G3",
        "G03",
        "G4",
        "G04",
        "G5",
        "G05",
        "G6",
        "G06",
        "G7",
        "G07",
        "G8",
        "G08",
        "G9",
        "G09",
        "G10",
        "G11",
        "G12",
        "H1",
        "H01",
        "H2",
        "H02",
        "H3",
        "H03",
        "H4",
        "H04",
        "H5",
        "H05",
        "H6",
        "H06",
        "H7",
        "H07",
        "H8",
        "H08",
        "H9",
        "H09",
        "H10",
        "H11",
        "H12",
    ],
)
def test_validate_generates_no_errors_for_valid_plate_coordinates(subject, create_message, plate_coordinate):
    assert_validate_when_message_is_valid(subject, create_message, plate_coordinate=plate_coordinate)


@pytest.mark.parametrize(
    "tested_date",
    [datetime(2022, 2, 14, 7, 24, 35), datetime(2021, 12, 31, 23, 59, 59), datetime(2022, 2, 13, 14, 30, 0)],
)
def test_validate_generates_no_errors_for_valid_tested_dates(subject, create_message, tested_date):
    assert_validate_when_message_is_valid(subject, create_message, tested_date=tested_date)


def test_validate_adds_error_when_lab_id_not_enabled(subject, create_message, add_error):
    create_message._body[FIELD_PLATE][FIELD_LAB_ID] = "NOT_A_CENTRE"

    subject.validate()

    add_error.assert_called_once_with(
        CreatePlateError(
            origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
            description="The lab ID provided 'NOT_A_CENTRE' is not configured to receive messages via RabbitMQ.",
            field=FIELD_LAB_ID,
        )
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 3


def test_validate_adds_error_when_plate_barcode_is_empty(subject, create_message, add_error):
    create_message._body[FIELD_PLATE][FIELD_PLATE_BARCODE] = ""

    subject.validate()

    add_error.assert_called_once_with(
        CreatePlateError(
            origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
            description="Field value is not populated.",
            long_description="Value for field 'plateBarcode' has not been populated.",
            field=FIELD_PLATE_BARCODE,
        )
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 3


def test_validate_adds_single_error_when_multiple_samples_have_the_same_uuid(subject, create_message, add_error):
    sample_uuid = "01234567-89ab-cdef-0123-456789abcdef"
    for sample in create_message._body[FIELD_PLATE][FIELD_SAMPLES]:
        sample[FIELD_SAMPLE_UUID] = sample_uuid.encode()

    subject.validate()

    add_error.assert_called_once_with(
        CreatePlateError(
            origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
            description=f"Sample UUID {sample_uuid} exists more than once in the message.",
            sample_uuid=sample_uuid,
            field=FIELD_SAMPLE_UUID,
        )
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 0


def test_validate_adds_error_when_root_sample_id_is_empty(subject, create_message, add_error):
    samples = create_message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0][FIELD_ROOT_SAMPLE_ID] = ""
    samples[1][FIELD_ROOT_SAMPLE_ID] = ""

    subject.validate()

    # We're only expecting 2 calls.  There should not be a call indicating that the empty values are not unique.
    add_error.assert_has_calls(
        [
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not populated.",
                    long_description=(
                        "Value for field 'rootSampleId' on sample '0aae6004-8e01-4f7a-9d50-91c51052813f' "
                        "has not been populated."
                    ),
                    sample_uuid=samples[0][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_ROOT_SAMPLE_ID,
                )
            ),
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not populated.",
                    long_description=(
                        "Value for field 'rootSampleId' on sample 'a9071f9c-0e3c-42c9-bef2-1045e827f9df' "
                        "has not been populated."
                    ),
                    sample_uuid=samples[1][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_ROOT_SAMPLE_ID,
                )
            ),
        ]
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 1


def test_validate_adds_error_when_root_sample_id_is_not_unique(subject, create_message, add_error):
    samples = create_message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0][FIELD_ROOT_SAMPLE_ID] = "ROOT-SAMPLE-ID"
    samples[1][FIELD_ROOT_SAMPLE_ID] = "ROOT-SAMPLE-ID"

    subject.validate()

    add_error.assert_has_calls(
        [
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not unique across samples (ROOT-SAMPLE-ID).",
                    long_description=(
                        "Field 'rootSampleId' on sample '0aae6004-8e01-4f7a-9d50-91c51052813f' contains the value "
                        "'ROOT-SAMPLE-ID' which is used in more than one sample but should be unique."
                    ),
                    sample_uuid=samples[0][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_ROOT_SAMPLE_ID,
                )
            ),
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not unique across samples (ROOT-SAMPLE-ID).",
                    long_description=(
                        "Field 'rootSampleId' on sample 'a9071f9c-0e3c-42c9-bef2-1045e827f9df' contains the value "
                        "'ROOT-SAMPLE-ID' which is used in more than one sample but should be unique."
                    ),
                    sample_uuid=samples[1][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_ROOT_SAMPLE_ID,
                )
            ),
        ]
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 1


def test_validate_adds_error_when_rna_id_is_empty(subject, create_message, add_error):
    samples = create_message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0][FIELD_RNA_ID] = ""
    samples[1][FIELD_RNA_ID] = ""

    subject.validate()

    # We're only expecting 2 calls.  There should not be a call indicating that the empty values are not unique.
    add_error.assert_has_calls(
        [
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not populated.",
                    long_description=(
                        "Value for field 'rnaId' on sample '0aae6004-8e01-4f7a-9d50-91c51052813f' "
                        "has not been populated."
                    ),
                    sample_uuid=samples[0][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_RNA_ID,
                )
            ),
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not populated.",
                    long_description=(
                        "Value for field 'rnaId' on sample 'a9071f9c-0e3c-42c9-bef2-1045e827f9df' "
                        "has not been populated."
                    ),
                    sample_uuid=samples[1][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_RNA_ID,
                )
            ),
        ]
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 1


def test_validate_adds_error_when_rna_id_is_not_unique(subject, create_message, add_error):
    samples = create_message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0][FIELD_RNA_ID] = "RNA-ID"
    samples[1][FIELD_RNA_ID] = "RNA-ID"

    subject.validate()

    add_error.assert_has_calls(
        [
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not unique across samples (RNA-ID).",
                    long_description=(
                        "Field 'rnaId' on sample '0aae6004-8e01-4f7a-9d50-91c51052813f' contains the value "
                        "'RNA-ID' which is used in more than one sample but should be unique."
                    ),
                    sample_uuid=samples[0][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_RNA_ID,
                )
            ),
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not unique across samples (RNA-ID).",
                    long_description=(
                        "Field 'rnaId' on sample 'a9071f9c-0e3c-42c9-bef2-1045e827f9df' contains the value "
                        "'RNA-ID' which is used in more than one sample but should be unique."
                    ),
                    sample_uuid=samples[1][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_RNA_ID,
                )
            ),
        ]
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 1


def test_validate_adds_error_when_cog_uk_id_is_empty(subject, create_message, add_error):
    samples = create_message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0][FIELD_COG_UK_ID] = ""
    samples[1][FIELD_COG_UK_ID] = ""

    subject.validate()

    # We're only expecting 2 calls.  There should not be a call indicating that the empty values are not unique.
    add_error.assert_has_calls(
        [
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not populated.",
                    long_description=(
                        "Value for field 'cogUkId' on sample '0aae6004-8e01-4f7a-9d50-91c51052813f' "
                        "has not been populated."
                    ),
                    sample_uuid=samples[0][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_COG_UK_ID,
                )
            ),
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not populated.",
                    long_description=(
                        "Value for field 'cogUkId' on sample 'a9071f9c-0e3c-42c9-bef2-1045e827f9df' "
                        "has not been populated."
                    ),
                    sample_uuid=samples[1][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_COG_UK_ID,
                )
            ),
        ]
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 1


def test_validate_adds_error_when_cog_uk_id_is_not_unique(subject, create_message, add_error):
    samples = create_message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0][FIELD_COG_UK_ID] = "COG-UK-ID"
    samples[1][FIELD_COG_UK_ID] = "COG-UK-ID"

    subject.validate()

    add_error.assert_has_calls(
        [
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not unique across samples (COG-UK-ID).",
                    long_description=(
                        "Field 'cogUkId' on sample '0aae6004-8e01-4f7a-9d50-91c51052813f' contains the value "
                        "'COG-UK-ID' which is used in more than one sample but should be unique."
                    ),
                    sample_uuid=samples[0][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_COG_UK_ID,
                )
            ),
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value is not unique across samples (COG-UK-ID).",
                    long_description=(
                        "Field 'cogUkId' on sample 'a9071f9c-0e3c-42c9-bef2-1045e827f9df' contains the value "
                        "'COG-UK-ID' which is used in more than one sample but should be unique."
                    ),
                    sample_uuid=samples[1][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_COG_UK_ID,
                )
            ),
        ]
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 1


@pytest.mark.parametrize("invalid_column", ["", "001", "0", "00", "13", "013", "A"])
def test_validate_adds_error_when_plate_coordinate_column_invalid(subject, create_message, add_error, invalid_column):
    samples = create_message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0][FIELD_PLATE_COORDINATE] = f"A{invalid_column}"
    samples[1][FIELD_PLATE_COORDINATE] = f"A{invalid_column}"

    subject.validate()

    # We're only expecting 2 calls.  There should not be a call indicating that the empty values are not unique.
    add_error.assert_has_calls(
        [
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value does not match regex (^[A-H](?:0?[1-9]|1[0-2])$).",
                    long_description=(
                        "Field 'plateCoordinate' on sample '0aae6004-8e01-4f7a-9d50-91c51052813f' contains the value "
                        f"'A{invalid_column}' which doesn't match the expected format for values in this field."
                    ),
                    sample_uuid=samples[0][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_PLATE_COORDINATE,
                )
            ),
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value does not match regex (^[A-H](?:0?[1-9]|1[0-2])$).",
                    long_description=(
                        "Field 'plateCoordinate' on sample 'a9071f9c-0e3c-42c9-bef2-1045e827f9df' contains the value "
                        f"'A{invalid_column}' which doesn't match the expected format for values in this field."
                    ),
                    sample_uuid=samples[1][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_PLATE_COORDINATE,
                )
            ),
        ]
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 1


@pytest.mark.parametrize(
    "invalid_row",
    ["", "0", "01", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"],
)
def test_validate_adds_error_when_plate_coordinate_row_invalid(subject, create_message, add_error, invalid_row):
    samples = create_message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0][FIELD_PLATE_COORDINATE] = f"{invalid_row}03"
    samples[1][FIELD_PLATE_COORDINATE] = f"{invalid_row}03"

    subject.validate()

    # We're only expecting 2 calls.  There should not be a call indicating that the empty values are not unique.
    add_error.assert_has_calls(
        [
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value does not match regex (^[A-H](?:0?[1-9]|1[0-2])$).",
                    long_description=(
                        "Field 'plateCoordinate' on sample '0aae6004-8e01-4f7a-9d50-91c51052813f' contains the value "
                        f"'{invalid_row}03' which doesn't match the expected format for values in this field."
                    ),
                    sample_uuid=samples[0][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_PLATE_COORDINATE,
                )
            ),
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description="Field value does not match regex (^[A-H](?:0?[1-9]|1[0-2])$).",
                    long_description=(
                        "Field 'plateCoordinate' on sample 'a9071f9c-0e3c-42c9-bef2-1045e827f9df' contains the value "
                        f"'{invalid_row}03' which doesn't match the expected format for values in this field."
                    ),
                    sample_uuid=samples[1][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_PLATE_COORDINATE,
                )
            ),
        ]
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 1


@pytest.mark.parametrize("first_coordinate, second_coordinate", [("E06", "E06"), ("E6", "E06"), ("E06", "E6")])
def test_validate_adds_error_when_plate_coordinate_is_not_unique(
    subject, create_message, add_error, first_coordinate, second_coordinate
):
    samples = create_message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0][FIELD_PLATE_COORDINATE] = first_coordinate
    samples[1][FIELD_PLATE_COORDINATE] = second_coordinate

    subject.validate()

    add_error.assert_has_calls(
        [
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description=f"Field value is not unique across samples ({first_coordinate}).",
                    long_description=(
                        "Field 'plateCoordinate' on sample '0aae6004-8e01-4f7a-9d50-91c51052813f' contains the value "
                        f"'{first_coordinate}' which is used in more than one sample but should be unique."
                    ),
                    sample_uuid=samples[0][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_PLATE_COORDINATE,
                )
            ),
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description=f"Field value is not unique across samples ({second_coordinate}).",
                    long_description=(
                        "Field 'plateCoordinate' on sample 'a9071f9c-0e3c-42c9-bef2-1045e827f9df' contains the value "
                        f"'{second_coordinate}' which is used in more than one sample but should be unique."
                    ),
                    sample_uuid=samples[1][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_PLATE_COORDINATE,
                )
            ),
        ]
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 1


def test_validate_adds_error_when_tested_date_is_too_recent(subject, create_message, add_error):
    create_message._body[FIELD_MESSAGE_CREATE_DATE] = datetime(2022, 4, 29, 12, 34, 56)
    samples = create_message._body[FIELD_PLATE][FIELD_SAMPLES]
    samples[0][FIELD_TESTED_DATE] = datetime(2022, 4, 29, 12, 34, 57)  # one second too late
    samples[1][FIELD_TESTED_DATE] = datetime(2023, 4, 29, 12, 34, 56)  # one year too late
    samples[2][FIELD_TESTED_DATE] = datetime(2022, 4, 29, 12, 34, 56)  # this is OK

    subject.validate()

    add_error.assert_has_calls(
        [
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description=(
                        "Field value repesents a timestamp that is too recent "
                        "(2022-04-29 12:34:57 > 2022-04-29 12:34:56)."
                    ),
                    long_description=(
                        "Field 'testedDateUtc' on sample '0aae6004-8e01-4f7a-9d50-91c51052813f' contains the value "
                        "'2022-04-29 12:34:57' which is too recent and should be lower than '2022-04-29 12:34:56'."
                    ),
                    sample_uuid=samples[0][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_TESTED_DATE,
                )
            ),
            call(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description=(
                        "Field value repesents a timestamp that is too recent "
                        "(2023-04-29 12:34:56 > 2022-04-29 12:34:56)."
                    ),
                    long_description=(
                        "Field 'testedDateUtc' on sample 'a9071f9c-0e3c-42c9-bef2-1045e827f9df' contains the value "
                        "'2023-04-29 12:34:56' which is too recent and should be lower than '2022-04-29 12:34:56'."
                    ),
                    sample_uuid=samples[1][FIELD_SAMPLE_UUID].decode(),
                    field=FIELD_TESTED_DATE,
                )
            ),
        ]
    )

    assert create_message.total_samples == 3
    assert create_message.validated_samples == 1
