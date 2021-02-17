import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId

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
    FIELD_LAB_ID,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SOURCE,
    FIELD_UPDATED_AT,
    FIELD_MUST_SEQUENCE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    MLWH_COORDINATE,
    MLWH_CREATED_AT,
    MLWH_DATE_TESTED,
    MLWH_FILTERED_POSITIVE,
    MLWH_FILTERED_POSITIVE_TIMESTAMP,
    MLWH_FILTERED_POSITIVE_VERSION,
    MLWH_LAB_ID,
    MLWH_LH_SAMPLE_UUID,
    MLWH_LH_SOURCE_PLATE_UUID,
    MLWH_MONGODB_ID,
    MLWH_PLATE_BARCODE,
    MLWH_RESULT,
    MLWH_RNA_ID,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_SOURCE,
    MLWH_UPDATED_AT,
    MLWH_MUST_SEQUENCE,
    MLWH_PREFERENTIALLY_SEQUENCE,
)
from crawler.helpers.general_helpers import (
    create_source_plate_doc,
    get_config,
    get_dart_well_index,
    map_mongo_doc_to_dart_well_props,
    map_mongo_sample_to_mysql,
    parse_decimal128,
    unpad_coordinate,
    pad_coordinate,
)
from crawler.types import SampleDoc


def test_get_config():
    with pytest.raises(ModuleNotFoundError):
        get_config("x.y.z")


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
    doc_to_transform = {
        FIELD_MONGODB_ID: ObjectId("5f562d9931d9959b92544728"),
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
        FIELD_PLATE_BARCODE: "TC-rna-00000029",
        FIELD_COORDINATE: "H01",
        FIELD_RESULT: "Negative",
        FIELD_DATE_TESTED: date_tested,
        FIELD_SOURCE: "Test Centre",
        FIELD_LAB_ID: "TC",
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

    doc_to_transform = {
        FIELD_MONGODB_ID: ObjectId("5f562d9931d9959b92544728"),
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
        FIELD_PLATE_BARCODE: "TC-rna-00000029",
        FIELD_COORDINATE: "H01",
        FIELD_RESULT: "Negative",
        FIELD_DATE_TESTED: date_tested,
        FIELD_SOURCE: "Test Centre",
        FIELD_LAB_ID: "TC",
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
    ), "Expected to be unable to determine a well index for sample with coordinate column "
    "below accepted range"

    coordinate = "B15"
    assert (
        get_dart_well_index(coordinate) is None
    ), "Expected to be unable to determine a well index for sample with coordinate column "
    "above accepted range"

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
        FIELD_LAB_ID: "TC",
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


def test_create_source_plate_doc(freezer):
    """Tests for updating docs with source plate UUIDs."""
    now = datetime.now()
    test_uuid = uuid.uuid4()
    plate_barcode = "abc123"
    lab_id = "AP"

    with patch("crawler.file_processing.uuid.uuid4", return_value=test_uuid):
        source_plate = create_source_plate_doc(plate_barcode, lab_id)

        assert source_plate[FIELD_LH_SOURCE_PLATE_UUID] == str(test_uuid)
        assert source_plate[FIELD_BARCODE] == plate_barcode
        assert source_plate[FIELD_LAB_ID] == lab_id
        assert source_plate[FIELD_UPDATED_AT] == now
        assert source_plate[FIELD_CREATED_AT] == now


def test_is_sample_important(sample):
    return (sample[FIELD_RESULT] == POSITIVE_RESULT_VALUE) or (is_sample_priority(sample))


def test_is_sample_priority(sample):
    return (sample[FIELD_MUST_SEQUENCE] == True) or (sample[FIELD_PREFERENTIALLY_SEQUENCE] == True)


def test_is_sample_pickable(sample):
    return sample.get(FIELD_FILTERED_POSITIVE, False) or is_sample_priority(sample)


## Also a test for this:
## def test_map_mongo_doc_to_dart_well_props(sample: SampleDoc) -> DartWellProp:
