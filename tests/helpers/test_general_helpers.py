import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from functools import partial
from http import HTTPStatus
from unittest.mock import ANY, patch

import pytest
import responses
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from requests import ConnectionError

from crawler.constants import (
    DART_EMPTY_VALUE,
    DART_LAB_ID,
    DART_LH_SAMPLE_UUID,
    DART_RNA_ID,
    DART_ROOT_SAMPLE_ID,
    DART_STATE,
    DART_STATE_PICKABLE,
    FIELD_BARCODE,
    FIELD_COORDINATE,
    FIELD_CREATED_AT,
    FIELD_DATE_TESTED,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGO_LAB_ID,
    FIELD_MONGODB_ID,
    FIELD_MUST_SEQUENCE,
    FIELD_PLATE_BARCODE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SOURCE,
    FIELD_UPDATED_AT,
    MLWH_COORDINATE,
    MLWH_CREATED_AT,
    MLWH_DATE_TESTED,
    MLWH_FILTERED_POSITIVE,
    MLWH_FILTERED_POSITIVE_TIMESTAMP,
    MLWH_FILTERED_POSITIVE_VERSION,
    MLWH_IS_CURRENT,
    MLWH_LAB_ID,
    MLWH_LH_SAMPLE_UUID,
    MLWH_LH_SOURCE_PLATE_UUID,
    MLWH_MONGODB_ID,
    MLWH_MUST_SEQUENCE,
    MLWH_PLATE_BARCODE,
    MLWH_PREFERENTIALLY_SEQUENCE,
    MLWH_RESULT,
    MLWH_RNA_ID,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_SOURCE,
    MLWH_UPDATED_AT,
    RESULT_VALUE_POSITIVE,
)
from crawler.exceptions import BaracodaError
from crawler.helpers.general_helpers import (
    create_source_plate_doc,
    extract_duplicated_values,
    generate_baracoda_barcodes,
    get_dart_well_index,
    get_sftp_connection,
    is_found_in_list,
    is_sample_pickable,
    is_sample_positive,
    map_mongo_doc_to_dart_well_props,
    map_mongo_sample_to_mysql,
    pad_coordinate,
    parse_decimal128,
    set_is_current_on_mysql_samples,
    unpad_coordinate,
)
from crawler.types import SampleDoc
from tests.conftest import generate_new_object_for_string


@pytest.fixture
def sftp_connection_class():
    with patch("pysftp.Connection") as connection:
        yield connection


@pytest.fixture
def mocked_responses():
    """Easily mock responses from HTTP calls.
    https://github.com/getsentry/responses#responses-as-a-pytest-fixture"""
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.mark.parametrize("given_username, expected_username", [[None, "foo"], ["", "foo"], ["username", "username"]])
@pytest.mark.parametrize("given_password, expected_password", [[None, "pass"], ["", "pass"], ["password", "password"]])
def test_get_sftp_connection(
    config, given_username, expected_username, given_password, expected_password, sftp_connection_class
):
    actual = get_sftp_connection(config, username=given_username, password=given_password)

    assert actual == sftp_connection_class.return_value

    sftp_connection_class.assert_called_once_with(
        host=config.SFTP_HOST, port=config.SFTP_PORT, username=expected_username, password=expected_password, cnopts=ANY
    )


@pytest.mark.parametrize("count", [2, 3])
@pytest.mark.parametrize("prefix", ("TEST", "ALDP"))
def test_generate_baracoda_barcodes_working_fine(config, count, prefix, mocked_responses):
    expected = [f"{prefix}-012345", f"{prefix}-012346", f"{prefix}-012347"]
    baracoda_url = f"{config.BARACODA_BASE_URL}/barcodes_group/{prefix}/new?count={count}"

    mocked_responses.add(
        responses.POST,
        baracoda_url,
        json={"barcodes_group": {"barcodes": expected}},
        status=HTTPStatus.CREATED,
    )

    actual = generate_baracoda_barcodes(config, prefix, count)

    assert actual == expected
    assert len(mocked_responses.calls) == 1


@pytest.mark.parametrize("count", [2, 3])
@pytest.mark.parametrize("prefix", ("TEST", "ALDP"))
def test_generate_baracoda_barcodes_will_retry_if_fail(config, count, prefix, mocked_responses):
    baracoda_url = f"{config.BARACODA_BASE_URL}/barcodes_group/{prefix}/new?count={count}"

    mocked_responses.add(
        responses.POST,
        baracoda_url,
        json={"errors": ["Some error from baracoda"]},
        status=HTTPStatus.INTERNAL_SERVER_ERROR,
    )

    with pytest.raises(BaracodaError):
        generate_baracoda_barcodes(config, prefix, count)

    assert len(mocked_responses.calls) == config.BARACODA_RETRY_ATTEMPTS


@pytest.mark.parametrize("count", [2, 3])
@pytest.mark.parametrize("prefix", ("TEST", "ALDP"))
@pytest.mark.parametrize("exception_type", [ConnectionError, Exception])
def test_generate_baracoda_barcodes_will_retry_if_exception(config, count, prefix, exception_type, mocked_responses):
    baracoda_url = f"{config.BARACODA_BASE_URL}/barcodes_group/{prefix}/new?count={count}"

    mocked_responses.add(
        responses.POST,
        baracoda_url,
        body=exception_type("Some error"),
        status=HTTPStatus.INTERNAL_SERVER_ERROR,
    )

    with pytest.raises(BaracodaError):
        generate_baracoda_barcodes(config, prefix, count)

    assert len(mocked_responses.calls) == config.BARACODA_RETRY_ATTEMPTS


@pytest.mark.parametrize("count", [2, 3])
@pytest.mark.parametrize("prefix", ("TEST", "ALDP"))
def test_generate_baracoda_barcodes_will_not_raise_error_if_success_after_retry(
    config, count, prefix, mocked_responses
):
    expected = [f"{prefix}-012345", f"{prefix}-012346", f"{prefix}-012347"]
    baracoda_url = f"{config.BARACODA_BASE_URL}/barcodes_group/{prefix}/new?count={count}"

    def request_callback(request, data):
        data["calls"] = data["calls"] + 1

        if data["calls"] == config.BARACODA_RETRY_ATTEMPTS:
            return (
                HTTPStatus.CREATED,
                {},
                json.dumps({"barcodes_group": {"barcodes": expected}}),
            )
        return (
            HTTPStatus.INTERNAL_SERVER_ERROR,
            {},
            json.dumps({"errors": ["Some error from baracoda"]}),
        )

    mocked_responses.add_callback(
        responses.POST,
        baracoda_url,
        callback=partial(request_callback, data={"calls": 0}),
        content_type="application/json",
    )

    generate_baracoda_barcodes(config, prefix, count)

    assert len(mocked_responses.calls) == config.BARACODA_RETRY_ATTEMPTS


# tests for parsing Decimal128
def test_parse_decimal128(config):
    result = parse_decimal128(None)
    assert result is None


def test_parse_decimal128_one(config):
    result = parse_decimal128("")
    assert result is None


def test_parse_decimal128_two(config):
    result = parse_decimal128(Decimal128("23.26273818"))
    assert result == Decimal("23.26273818")


# tests for unpad coordinate
def test_unpad_coordinate():
    assert unpad_coordinate("A01") == "A1"
    assert unpad_coordinate("A1") == "A1"
    assert unpad_coordinate("A10") == "A10"
    assert unpad_coordinate("B01010") == "B1010"


# tests for pad coordinate
def test_pad_coordinate():
    assert pad_coordinate("A1") == "A01"
    assert pad_coordinate("A01") == "A01"
    assert pad_coordinate("A10") == "A10"
    assert pad_coordinate("B01010") == "B01010"


def test_map_mongo_sample_to_mysql(config):
    """Tests for lighthouse doc to MLWH mapping"""
    date_tested = datetime(2020, 4, 23, 14, 40, 8)
    filtered_positive_timestamp = datetime(2020, 4, 24, 14, 0, 8)
    doc_to_transform: SampleDoc = {
        FIELD_MONGODB_ID: ObjectId("5f562d9931d9959b92544728"),
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
        FIELD_PLATE_BARCODE: "TC-rna-00000029",
        FIELD_COORDINATE: "H01",
        FIELD_RESULT: "Negative",
        FIELD_DATE_TESTED: date_tested,
        FIELD_SOURCE: "Test Centre",
        FIELD_MONGO_LAB_ID: "TC",
        FIELD_FILTERED_POSITIVE: True,
        FIELD_FILTERED_POSITIVE_VERSION: "v2.3",
        FIELD_FILTERED_POSITIVE_TIMESTAMP: filtered_positive_timestamp,
        FIELD_LH_SAMPLE_UUID: "7512638d-f25e-4ef0-85f0-d921d5263449",
        FIELD_LH_SOURCE_PLATE_UUID: "88ed5139-9e0c-4118-8cc8-20413b9ffa01",
        FIELD_MUST_SEQUENCE: True,
        FIELD_PREFERENTIALLY_SEQUENCE: False,
    }

    result = map_mongo_sample_to_mysql(doc_to_transform)

    assert result[MLWH_MONGODB_ID] == "5f562d9931d9959b92544728"
    assert result[MLWH_ROOT_SAMPLE_ID] == "ABC00000004"
    assert result[MLWH_RNA_ID] == "TC-rna-00000029_H01"
    assert result[MLWH_PLATE_BARCODE] == "TC-rna-00000029"
    assert result[MLWH_COORDINATE] == "H1"
    assert result[MLWH_RESULT] == "Negative"
    assert result[MLWH_DATE_TESTED] == date_tested
    assert result[MLWH_SOURCE] == "Test Centre"
    assert result[MLWH_LAB_ID] == "TC"
    assert result[MLWH_FILTERED_POSITIVE] is True
    assert result[MLWH_FILTERED_POSITIVE_VERSION] == "v2.3"
    assert result[MLWH_FILTERED_POSITIVE_TIMESTAMP] == filtered_positive_timestamp
    assert result[MLWH_LH_SAMPLE_UUID] == "7512638d-f25e-4ef0-85f0-d921d5263449"
    assert result[MLWH_LH_SOURCE_PLATE_UUID] == "88ed5139-9e0c-4118-8cc8-20413b9ffa01"
    assert result.get(MLWH_CREATED_AT) is not None
    assert result.get(MLWH_UPDATED_AT) is not None
    assert result[MLWH_MUST_SEQUENCE] is True
    assert result[MLWH_PREFERENTIALLY_SEQUENCE] is False


def test_map_mongo_sample_to_mysql_with_copy(config):
    date_tested = datetime(2020, 4, 23, 14, 40, 8)
    created_at = datetime(2020, 4, 27, 5, 20, 0, tzinfo=timezone.utc)
    updated_at = datetime(2020, 5, 13, 12, 50, 0, tzinfo=timezone.utc)

    doc_to_transform: SampleDoc = {
        FIELD_MONGODB_ID: ObjectId("5f562d9931d9959b92544728"),
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
        FIELD_PLATE_BARCODE: "TC-rna-00000029",
        FIELD_COORDINATE: "H01",
        FIELD_RESULT: "Negative",
        FIELD_DATE_TESTED: date_tested,
        FIELD_SOURCE: "Test Centre",
        FIELD_MONGO_LAB_ID: "TC",
        FIELD_CREATED_AT: created_at,
        FIELD_UPDATED_AT: updated_at,
    }

    result = map_mongo_sample_to_mysql(doc_to_transform, copy_date=True)

    assert result[MLWH_MONGODB_ID] == "5f562d9931d9959b92544728"
    assert result[MLWH_ROOT_SAMPLE_ID] == "ABC00000004"
    assert result[MLWH_RNA_ID] == "TC-rna-00000029_H01"
    assert result[MLWH_PLATE_BARCODE] == "TC-rna-00000029"
    assert result[MLWH_COORDINATE] == "H1"
    assert result[MLWH_RESULT] == "Negative"
    assert result[MLWH_DATE_TESTED] == date_tested
    assert result[MLWH_SOURCE] == "Test Centre"
    assert result[MLWH_LAB_ID] == "TC"
    assert result[MLWH_LH_SAMPLE_UUID] is None
    assert result[MLWH_LH_SOURCE_PLATE_UUID] is None
    assert result[MLWH_CREATED_AT] == created_at
    assert result[MLWH_UPDATED_AT] == updated_at


def test_set_is_current_on_mysql_samples_no_duplicates():
    input_samples = [
        {MLWH_RNA_ID: "rna_A01"},
        {MLWH_RNA_ID: "rna_D07"},
        {MLWH_RNA_ID: "rna_H12"},
    ]
    output_samples = set_is_current_on_mysql_samples(input_samples)

    # Input samples were not updated -- don't mutate what you were passed
    assert not any(MLWH_IS_CURRENT in sample for sample in input_samples)

    # Output samples were updated to all have is_current and are in input order
    assert [sample[MLWH_RNA_ID] for sample in output_samples] == ["rna_A01", "rna_D07", "rna_H12"]
    assert all(sample[MLWH_IS_CURRENT] for sample in output_samples)


def test_set_is_current_on_mysql_samples_with_duplicates():
    input_samples = [
        {MLWH_RNA_ID: "rna_A01"},
        {MLWH_RNA_ID: "rna_D07"},
        {MLWH_RNA_ID: "rna_H12"},
        {MLWH_RNA_ID: "rna_D07"},
    ]
    output_samples = set_is_current_on_mysql_samples(input_samples)

    # Input samples were not updated -- don't mutate what you were passed
    assert not any(MLWH_IS_CURRENT in sample for sample in input_samples)

    # Output samples were updated to have correct is_current values and correct order
    assert [sample[MLWH_RNA_ID] for sample in output_samples] == ["rna_A01", "rna_D07", "rna_H12", "rna_D07"]
    assert [sample[MLWH_IS_CURRENT] for sample in output_samples] == [True, False, True, True]


def test_set_is_current_on_mysql_samples_missing_rna_ids():
    input_samples = [
        {MLWH_RNA_ID: "rna_A01"},
        {"id": "test"},
        {MLWH_RNA_ID: ""},
        {MLWH_RNA_ID: "rna_H12"},
    ]
    output_samples = set_is_current_on_mysql_samples(input_samples)

    # Input samples were not updated -- don't mutate what you were passed
    assert not any(MLWH_IS_CURRENT in sample for sample in input_samples)

    # Output samples were updated to have correct is_current values and correct order
    assert [sample[MLWH_RNA_ID] for sample in output_samples if MLWH_RNA_ID in sample] == ["rna_A01", "", "rna_H12"]
    assert [sample[MLWH_IS_CURRENT] for sample in output_samples] == [True, False, False, True]


def test_get_dart_well_index(config):
    coordinate = None
    assert get_dart_well_index(coordinate) is None, "Expected to be unable to determine a well index for no sample"

    coordinate = "01A"
    assert (
        get_dart_well_index(coordinate) is None
    ), "Expected to be unable to determine a well index for sample with invalid coordinate"

    coordinate = "A00"
    assert (
        get_dart_well_index(coordinate) is None
    ), "Expected to be unable to determine a well index for sample with coordinate column below accepted range"

    coordinate = "B15"
    assert (
        get_dart_well_index(coordinate) is None
    ), "Expected to be unable to determine a well index for sample with coordinate column above accepted range"

    coordinate = "Q01"
    assert (
        get_dart_well_index(coordinate) is None
    ), "Expected to be unable to determine a well index for sample with coordinate row out of range"

    coordinate = "B7"
    assert get_dart_well_index(coordinate) == 19, "Expected well index of 19"

    coordinate = "F03"
    assert get_dart_well_index(coordinate) == 63, "Expected well index of 63"

    coordinate = "H11"
    assert get_dart_well_index(coordinate) == 95, "Expected well index of 95"


def test_map_mongo_doc_to_dart_well_props(config):
    test_uuid = str(uuid.uuid4())

    # all fields present, filtered positive
    doc_to_transform: SampleDoc = {
        FIELD_FILTERED_POSITIVE: True,
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
        FIELD_MONGO_LAB_ID: "TC",
        FIELD_LH_SAMPLE_UUID: test_uuid,
    }

    result = map_mongo_doc_to_dart_well_props(doc_to_transform)

    assert result[DART_STATE] == DART_STATE_PICKABLE
    assert result[DART_ROOT_SAMPLE_ID] == "ABC00000004"
    assert result[DART_RNA_ID] == "TC-rna-00000029_H01"
    assert result[DART_LAB_ID] == "TC"
    assert result[DART_LH_SAMPLE_UUID] == test_uuid

    # missing lab id and sample uuid, not a filtered positive
    doc_to_transform = {
        FIELD_FILTERED_POSITIVE: False,
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
    }

    result = map_mongo_doc_to_dart_well_props(doc_to_transform)

    assert result[DART_STATE] == DART_EMPTY_VALUE
    assert result[DART_LAB_ID] == DART_EMPTY_VALUE
    assert result[DART_LH_SAMPLE_UUID] == DART_EMPTY_VALUE

    # missing filtered positive
    doc_to_transform = {
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
    }

    result = map_mongo_doc_to_dart_well_props(doc_to_transform)

    assert result[DART_STATE] == DART_EMPTY_VALUE

    # is pickable as filtered positive is false, but must sequence is true
    doc_to_transform = {
        FIELD_FILTERED_POSITIVE: False,
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_MUST_SEQUENCE: True,
        FIELD_RNA_ID: "TC-rna-00000029_H01",
    }

    result = map_mongo_doc_to_dart_well_props(doc_to_transform)

    assert result[DART_STATE] == DART_STATE_PICKABLE
    assert result[DART_ROOT_SAMPLE_ID] == "ABC00000004"
    assert result[DART_RNA_ID] == "TC-rna-00000029_H01"

    # is not pickable as filtered positive is false, but preferentially sequence is true
    doc_to_transform = {
        FIELD_FILTERED_POSITIVE: False,
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_PREFERENTIALLY_SEQUENCE: True,
        FIELD_RNA_ID: "TC-rna-00000029_H01",
    }

    result = map_mongo_doc_to_dart_well_props(doc_to_transform)

    assert result[DART_STATE] == DART_EMPTY_VALUE
    assert result[DART_ROOT_SAMPLE_ID] == "ABC00000004"
    assert result[DART_RNA_ID] == "TC-rna-00000029_H01"


def test_create_source_plate_doc(freezer):
    """Tests for updating docs with source plate UUIDs."""
    now = datetime.now(tz=timezone.utc)
    test_uuid = uuid.uuid4()
    plate_barcode = "abc123"
    lab_id = "AP"

    with patch("crawler.file_processing.uuid.uuid4", return_value=test_uuid):
        source_plate = create_source_plate_doc(plate_barcode, lab_id)

        assert source_plate[FIELD_LH_SOURCE_PLATE_UUID] == str(test_uuid)
        assert source_plate[FIELD_BARCODE] == plate_barcode
        assert source_plate[FIELD_MONGO_LAB_ID] == lab_id
        assert source_plate[FIELD_UPDATED_AT] == now
        assert source_plate[FIELD_CREATED_AT] == now


def test_is_sample_positive():
    assert is_sample_positive({FIELD_RESULT: "negative"}) is False
    assert is_sample_positive({FIELD_RESULT: RESULT_VALUE_POSITIVE}) is True
    assert is_sample_positive({FIELD_RESULT: generate_new_object_for_string(RESULT_VALUE_POSITIVE)}) is True


def test_is_sample_pickable():
    assert (
        is_sample_pickable(
            {FIELD_FILTERED_POSITIVE: True, FIELD_MUST_SEQUENCE: False, FIELD_PREFERENTIALLY_SEQUENCE: False}
        )
        is True
    )
    assert (
        is_sample_pickable(
            {FIELD_FILTERED_POSITIVE: True, FIELD_MUST_SEQUENCE: True, FIELD_PREFERENTIALLY_SEQUENCE: False}
        )
        is True
    )
    assert (
        is_sample_pickable(
            {FIELD_FILTERED_POSITIVE: True, FIELD_MUST_SEQUENCE: True, FIELD_PREFERENTIALLY_SEQUENCE: True}
        )
        is True
    )
    assert (
        is_sample_pickable(
            {FIELD_FILTERED_POSITIVE: False, FIELD_MUST_SEQUENCE: True, FIELD_PREFERENTIALLY_SEQUENCE: False}
        )
        is True
    )
    assert (
        is_sample_pickable(
            {FIELD_FILTERED_POSITIVE: False, FIELD_MUST_SEQUENCE: False, FIELD_PREFERENTIALLY_SEQUENCE: True}
        )
        is False
    )
    assert (
        is_sample_pickable(
            {FIELD_FILTERED_POSITIVE: False, FIELD_MUST_SEQUENCE: False, FIELD_PREFERENTIALLY_SEQUENCE: False}
        )
        is False
    )
    assert is_sample_pickable({FIELD_FILTERED_POSITIVE: True}) is True
    assert is_sample_pickable({FIELD_MUST_SEQUENCE: True}) is True
    assert is_sample_pickable({FIELD_PREFERENTIALLY_SEQUENCE: True}) is False


@pytest.mark.parametrize(
    "input, expected",
    [
        [[], set()],
        [["one", "two", "three", "four"], set()],
        [["one", "two", "three", "two"], set(["two"])],
        [["one", "two", "three", "two", "four", "two", "one"], set(["one", "two"])],
    ],
)
def test_extract_duplicated_values_gives_correct_result(input, expected):
    assert extract_duplicated_values(input) == expected


@pytest.mark.parametrize(
    "needle, haystack, expected",
    [
        ["two", ["one", "two", "three", "four"], True],
        ["one", [], False],
        ["ten", ["one", "two", "three", "two", "four", "two", "one"], False],
    ],
)
def test_is_found_in_list_gives_correct_result(needle, haystack, expected):
    assert is_found_in_list(needle, haystack) is expected
