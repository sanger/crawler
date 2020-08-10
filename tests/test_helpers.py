import os
import pytest
from crawler.helpers import LoggingCollection
from unittest.mock import patch

from crawler.constants import (
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
)
from crawler.helpers import get_config


def test_get_config():
    with pytest.raises(ModuleNotFoundError):
        get_config("x.y.z")


def test_logging_collection_with_a_single_error():
    logging = LoggingCollection()
    logging.add_error("TYPE 3", "This is a testing message")
    aggregator = logging.aggregator_types["TYPE 3"]
    assert aggregator.count_errors == 1
    assert aggregator.max_errors == 5
    assert aggregator.get_report_message() == "Total number of Only root sample id errors: 1"
    exptd_msgs = "WARNING: Sample rows that have Root Sample ID value but no other information. (e.g. This is a testing message)"
    assert aggregator.get_message() == exptd_msgs
    assert logging.get_aggregate_messages() == [exptd_msgs]
    assert logging.get_count_of_all_errors_and_criticals() == 0
    assert logging.get_aggregate_total_messages() == [
        "Total number of Only root sample id errors: 1"
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
        "DEBUG: Blank rows in files.",
        "CRITICAL: Files where we do not have the expected main column headers of Root Sample ID, RNA ID and Result.",
        "WARNING: Sample rows that have Root Sample ID value but no other information. (e.g. This is the first type 3 message) (e.g. This is the second type 3 message) (e.g. This is the third type 3 message)",
        "ERROR: Sample rows that have Root Sample ID and Result values but no RNA ID (no plate barcode). (e.g. This is the first type 4 message)",
    ]
    assert logging.get_aggregate_messages() == exptd_msgs
    assert logging.get_count_of_all_errors_and_criticals() == 3

    exptd_report_msgs = [
        "Total number of Blank row errors: 2",
        "Total number of Missing header column errors: 2",
        "Total number of Only root sample id errors: 3",
        "Total number of No plate barcode errors: 1",
    ]
    assert logging.get_aggregate_total_messages() == exptd_report_msgs
