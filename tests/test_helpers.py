import os
import pytest
from crawler.helpers import LoggingCollection
from unittest.mock import patch
from decimal import Decimal
from bson.decimal128 import Decimal128  # type: ignore
from crawler.constants import (
    FIELD_MONGODB_ID,
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_PLATE_BARCODE,
    FIELD_COORDINATE,
    FIELD_SOURCE,
    FIELD_CREATED_AT,
    FIELD_UPDATED_AT,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    MLWH_MONGODB_ID,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_RNA_ID,
    MLWH_PLATE_BARCODE,
    MLWH_COORDINATE,
    MLWH_RESULT,
    MLWH_DATE_TESTED_STRING,
    MLWH_DATE_TESTED,
    MLWH_SOURCE,
    MLWH_LAB_ID,
    MLWH_CREATED_AT,
    MLWH_UPDATED_AT,
    MLWH_FILTERED_POSITIVE,
    MLWH_FILTERED_POSITIVE_VERSION,
    MLWH_FILTERED_POSITIVE_TIMESTAMP,
    MYSQL_DATETIME_FORMAT,
    DART_STATE,
    DART_ROOT_SAMPLE_ID,
    DART_RNA_ID,
    DART_LAB_ID,
    DART_STATE_PICKABLE,
    DART_EMPTY_VALUE,
)
from crawler.helpers import (
    parse_date_tested,
    parse_decimal128,
    get_config,
    map_lh_doc_to_sql_columns,
    map_mongo_doc_to_sql_columns,
    unpad_coordinate,
    get_dart_well_index,
    map_mongo_doc_to_dart_well_props,
)
from datetime import (
    datetime,
    timezone,
)
from bson.objectid import ObjectId


def test_get_config():
    with pytest.raises(ModuleNotFoundError):
        get_config("x.y.z")


def test_logging_collection_with_a_single_error():
    logging = LoggingCollection()
    logging.add_error("TYPE 3", "This is a testing message")
    aggregator = logging.aggregator_types["TYPE 3"]
    assert aggregator.count_errors == 1
    assert aggregator.max_errors == 5
    assert (
        aggregator.get_report_message() == "Total number of Only root sample id errors (TYPE 3): 1"
    )
    exptd_msgs = "WARNING: Sample rows that have Root Sample ID value but no other information. (TYPE 3) (e.g. This is a testing message)"
    assert aggregator.get_message() == exptd_msgs
    assert logging.get_aggregate_messages() == [exptd_msgs]
    assert logging.get_count_of_all_errors_and_criticals() == 0
    assert logging.get_aggregate_total_messages() == [
        "Total number of Only root sample id errors (TYPE 3): 1"
    ]


def test_logging_collection_with_multiple_errors():
    logging = LoggingCollection()
    logging.add_error("TYPE 3", "This is the first type 3 message")
    logging.add_error("TYPE 1", "This is the first type 1 message")
    logging.add_error("TYPE 2", "This is the first type 2 message")
    logging.add_error("TYPE 3", "This is the second type 3 message")
    logging.add_error("TYPE 2", "This is the second type 2 message")
    logging.add_error("TYPE 4", "This is the first type 4 message")
    logging.add_error("TYPE 1", "This is the first type 1 message")
    logging.add_error("TYPE 3", "This is the third type 3 message")

    aggregator_type_1 = logging.aggregator_types["TYPE 1"]
    aggregator_type_2 = logging.aggregator_types["TYPE 2"]
    aggregator_type_3 = logging.aggregator_types["TYPE 3"]
    aggregator_type_4 = logging.aggregator_types["TYPE 4"]

    assert aggregator_type_1.count_errors == 2
    assert aggregator_type_2.count_errors == 2
    assert aggregator_type_3.count_errors == 3
    assert aggregator_type_4.count_errors == 1

    exptd_msgs = [
        "DEBUG: Blank rows in files. (TYPE 1)",
        "CRITICAL: Files where we do not have the expected main column headers of Root Sample ID, RNA ID and Result. (TYPE 2)",
        "WARNING: Sample rows that have Root Sample ID value but no other information. (TYPE 3) (e.g. This is the first type 3 message) (e.g. This is the second type 3 message) (e.g. This is the third type 3 message)",
        "ERROR: Sample rows that have Root Sample ID and Result values but no RNA ID (no plate barcode). (TYPE 4) (e.g. This is the first type 4 message)",
    ]
    assert logging.get_aggregate_messages() == exptd_msgs
    assert logging.get_count_of_all_errors_and_criticals() == 3

    exptd_report_msgs = [
        "Total number of Blank row errors (TYPE 1): 2",
        "Total number of Missing header column errors (TYPE 2): 2",
        "Total number of Only root sample id errors (TYPE 3): 3",
        "Total number of No plate barcode errors (TYPE 4): 1",
    ]
    assert logging.get_aggregate_total_messages() == exptd_report_msgs


# tests for parsing date tested
def test_parse_date_tested(config):
    result = parse_date_tested(date_string="2020-11-02 13:04:23 UTC")
    assert result == datetime(2020, 11, 2, 13, 4, 23)


def test_parse_date_tested_none(config):
    result = parse_date_tested(date_string=None)
    assert result == None


def test_parse_date_tested_wrong_format(config):
    result = parse_date_tested(date_string="2nd November 2020")
    assert result == None


# tests for parsing Decimal128
def test_parse_decimal128(config):
    result = parse_decimal128(None)
    assert result == None


def test_parse_decimal128(config):
    result = parse_decimal128("")
    assert result == None


def test_parse_decimal128(config):
    result = parse_decimal128(Decimal128("23.26273818"))
    assert result == Decimal("23.26273818")


# tests for unpad coordinate
def test_unpad_coordinate_A01():
    assert unpad_coordinate("A01") == "A1"


def test_unpad_coordinate_A1():
    assert unpad_coordinate("A1") == "A1"


def test_unpad_coordinate_A10():
    assert unpad_coordinate("A10") == "A10"


def test_unpad_coordinate_B01010():
    assert unpad_coordinate("B01010") == "B1010"


# tests for lighthouse doc to MLWH mapping
def test_map_lh_doc_to_sql_columns(config):
    doc_to_transform = {
        FIELD_MONGODB_ID: ObjectId("5f562d9931d9959b92544728"),
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
        FIELD_PLATE_BARCODE: "TC-rna-00000029",
        FIELD_COORDINATE: "H01",
        FIELD_RESULT: "Negative",
        FIELD_DATE_TESTED: "2020-04-23 14:40:08 UTC",
        FIELD_SOURCE: "Test Centre",
        FIELD_LAB_ID: "TC",
        FIELD_FILTERED_POSITIVE: True,
        FIELD_FILTERED_POSITIVE_VERSION: "v2.3",
        FIELD_FILTERED_POSITIVE_TIMESTAMP: datetime(2020, 4, 23, 14, 40, 8),
    }

    result = map_lh_doc_to_sql_columns(doc_to_transform)

    assert result[MLWH_MONGODB_ID] == "5f562d9931d9959b92544728"
    assert result[MLWH_ROOT_SAMPLE_ID] == "ABC00000004"
    assert result[MLWH_RNA_ID] == "TC-rna-00000029_H01"
    assert result[MLWH_PLATE_BARCODE] == "TC-rna-00000029"
    assert result[MLWH_COORDINATE] == "H1"
    assert result[MLWH_RESULT] == "Negative"
    assert result[MLWH_DATE_TESTED_STRING] == "2020-04-23 14:40:08 UTC"
    assert result[MLWH_DATE_TESTED] == datetime(2020, 4, 23, 14, 40, 8)
    assert result[MLWH_SOURCE] == "Test Centre"
    assert result[MLWH_LAB_ID] == "TC"
    assert result[MLWH_FILTERED_POSITIVE] == True
    assert result[MLWH_FILTERED_POSITIVE_VERSION] == "v2.3"
    assert result[MLWH_FILTERED_POSITIVE_TIMESTAMP] == datetime(2020, 4, 23, 14, 40, 8)
    assert result.get(MLWH_CREATED_AT) is not None
    assert result.get(MLWH_UPDATED_AT) is not None


def test_map_mongo_doc_to_sql_columns(config):
    doc_to_transform = {
        FIELD_MONGODB_ID: ObjectId("5f562d9931d9959b92544728"),
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
        FIELD_PLATE_BARCODE: "TC-rna-00000029",
        FIELD_COORDINATE: "H01",
        FIELD_RESULT: "Negative",
        FIELD_DATE_TESTED: "2020-04-23 14:40:08 UTC",
        FIELD_SOURCE: "Test Centre",
        FIELD_LAB_ID: "TC",
        FIELD_CREATED_AT: datetime(2020, 4, 27, 5, 20, 0, tzinfo=timezone.utc),
        FIELD_UPDATED_AT: datetime(2020, 5, 13, 12, 50, 0, tzinfo=timezone.utc),
    }

    result = map_mongo_doc_to_sql_columns(doc_to_transform)

    assert result[MLWH_MONGODB_ID] == "5f562d9931d9959b92544728"
    assert result[MLWH_ROOT_SAMPLE_ID] == "ABC00000004"
    assert result[MLWH_RNA_ID] == "TC-rna-00000029_H01"
    assert result[MLWH_PLATE_BARCODE] == "TC-rna-00000029"
    assert result[MLWH_COORDINATE] == "H1"
    assert result[MLWH_RESULT] == "Negative"
    assert result[MLWH_DATE_TESTED_STRING] == "2020-04-23 14:40:08 UTC"
    assert result[MLWH_DATE_TESTED] == datetime(2020, 4, 23, 14, 40, 8)
    assert result[MLWH_SOURCE] == "Test Centre"
    assert result[MLWH_LAB_ID] == "TC"
    assert result[MLWH_CREATED_AT] == datetime(2020, 4, 27, 5, 20, 0, tzinfo=timezone.utc)
    assert result[MLWH_UPDATED_AT] == datetime(2020, 5, 13, 12, 50, 0, tzinfo=timezone.utc)


def test_get_dart_well_index(config):
    coordinate = None
    assert (
        get_dart_well_index(coordinate) == None
    ), "Expected to be unable to determine a well index for no sample"

    coordinate = "01A"
    assert (
        get_dart_well_index(coordinate) == None
    ), "Expected to be unable to determine a well index for sample with invalid coordinate"

    coordinate = "A00"
    assert (
        get_dart_well_index(coordinate) == None
    ), "Expected to be unable to determine a well index for sample with coordinate column below accepted range"

    coordinate = "B15"
    assert (
        get_dart_well_index(coordinate) == None
    ), "Expected to be unable to determine a well index for sample with coordinate column above accepted range"

    coordinate = "Q01"
    assert (
        get_dart_well_index(coordinate) == None
    ), "Expected to be unable to determine a well index for sample with coordinate row out of range"

    coordinate = "B7"
    assert get_dart_well_index(coordinate) == 19, "Expected well index of 19"

    coordinate = "F03"
    assert get_dart_well_index(coordinate) == 63, "Expected well index of 63"

    coordinate = "H11"
    assert get_dart_well_index(coordinate) == 95, "Expected well index of 95"


def test_map_mongo_doc_to_dart_well_props(config):
    # all fields present, filtered positive
    doc_to_transform = {
        FIELD_FILTERED_POSITIVE: True,
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
        FIELD_LAB_ID: "TC",
    }

    result = map_mongo_doc_to_dart_well_props(doc_to_transform)

    assert result[DART_STATE] == DART_STATE_PICKABLE
    assert result[DART_ROOT_SAMPLE_ID] == "ABC00000004"
    assert result[DART_RNA_ID] == "TC-rna-00000029_H01"
    assert result[DART_LAB_ID] == "TC"

    # missing lab id, not a filtered positive
    doc_to_transform = {
        FIELD_FILTERED_POSITIVE: False,
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
    }

    result = map_mongo_doc_to_dart_well_props(doc_to_transform)

    assert result[DART_STATE] == DART_EMPTY_VALUE
    assert result[DART_LAB_ID] == DART_EMPTY_VALUE

    # missing filtered positive
    doc_to_transform = {
        FIELD_ROOT_SAMPLE_ID: "ABC00000004",
        FIELD_RNA_ID: "TC-rna-00000029_H01",
    }

    result = map_mongo_doc_to_dart_well_props(doc_to_transform)

    assert result[DART_STATE] == DART_EMPTY_VALUE
