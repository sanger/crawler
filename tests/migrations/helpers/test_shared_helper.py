import uuid
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

from crawler.constants import (
    FIELD_COORDINATE,
    FIELD_CREATED_AT,
    FIELD_LAB_ID,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_UPDATED_AT,
)
from migrations.helpers.shared_helper import (
    extract_required_cp_info,
    get_cherrypicked_samples,
    remove_cherrypicked_samples,
)

# ----- test helpers -----


def generate_example_samples(range, start_datetime):
    samples = []
    #  create positive samples
    for n in range:
        samples.append(
            {
                FIELD_MONGODB_ID: str(uuid.uuid4()),
                FIELD_ROOT_SAMPLE_ID: f"TLS0000000{n}",
                FIELD_RESULT: "Positive",
                FIELD_PLATE_BARCODE: f"DN1000000{n}",
                FIELD_LAB_ID: "TLS",
                FIELD_RNA_ID: f"rna_{n}",
                FIELD_CREATED_AT: start_datetime + timedelta(days=n),
                FIELD_UPDATED_AT: start_datetime + timedelta(days=n),
            }
        )

    # create negative sample
    samples.append(
        {
            FIELD_MONGODB_ID: str(uuid.uuid4()),
            FIELD_ROOT_SAMPLE_ID: "TLS0000000_neg",
            FIELD_RESULT: "Negative",
            FIELD_PLATE_BARCODE: "DN10000000",
            FIELD_LAB_ID: "TLS",
            FIELD_RNA_ID: "rna_negative",
            FIELD_CREATED_AT: start_datetime,
            FIELD_UPDATED_AT: start_datetime,
        }
    )

    # create control sample
    samples.append(
        {
            FIELD_MONGODB_ID: str(uuid.uuid4()),
            FIELD_ROOT_SAMPLE_ID: "CBIQA_TLS0000000_control",
            FIELD_RESULT: "Positive",
            FIELD_PLATE_BARCODE: "DN10000000",
            FIELD_LAB_ID: "TLS",
            FIELD_RNA_ID: "rna_sample",
            FIELD_CREATED_AT: start_datetime,
            FIELD_UPDATED_AT: start_datetime,
        }
    )
    return samples


# ----- extract_required_cp_info tests -----


def test_extract_required_cp_info():
    test_samples = generate_example_samples(range(0, 3), datetime.now())
    test_samples.append(test_samples[0])

    expected_barcodes = set(["DN10000000", "DN10000001", "DN10000002"])
    expected_root_sample_ids = set(
        ["TLS00000000", "TLS00000001", "TLS00000002", "TLS0000000_neg", "CBIQA_TLS0000000_control"]
    )

    root_sample_ids, barcodes = extract_required_cp_info(test_samples)

    assert barcodes == expected_barcodes
    assert root_sample_ids == expected_root_sample_ids


# ----- remove_cherrypicked_samples tests -----


def test_remove_cherrypicked_samples():
    test_samples = generate_example_samples(range(0, 6), datetime.now())
    mock_cherry_picked_sample = [test_samples[0][FIELD_ROOT_SAMPLE_ID], test_samples[0][FIELD_PLATE_BARCODE]]

    samples = remove_cherrypicked_samples(test_samples, [mock_cherry_picked_sample])
    assert len(samples) == 7
    assert mock_cherry_picked_sample[0] not in [sample[FIELD_ROOT_SAMPLE_ID] for sample in samples]


# ----- get_cherrypicked_samples tests -----


# Test Scenario
# - Mocking database responses
# - Only the Sentinel query returns matches (No Beckman)
# - No chunking: a single query is made in which all matches are returned
# - No duplication of returned matches
def test_get_cherrypicked_samples_no_beckman(config):
    expected = [
        # Sentinel query response
        pd.DataFrame(["MCM001", "MCM003", "MCM005"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0, 1, 2]),
        # Beckman query response
        pd.DataFrame([]),
    ]
    samples = ["MCM001", "MCM002", "MCM003", "MCM004", "MCM005"]
    plate_barcodes = ["123", "456"]

    with patch("sqlalchemy.create_engine", return_value=Mock()):
        with patch("pandas.read_sql", side_effect=expected):
            returned_samples = get_cherrypicked_samples(config, samples, plate_barcodes)
            assert returned_samples.at[0, FIELD_ROOT_SAMPLE_ID] == "MCM001"  # type: ignore
            assert returned_samples.at[1, FIELD_ROOT_SAMPLE_ID] == "MCM003"  # type: ignore
            assert returned_samples.at[2, FIELD_ROOT_SAMPLE_ID] == "MCM005"  # type: ignore


# Test Scenario
# - Mocking database responses
# - Only the Sentinel queries return matches (No Beckman)
# - Chunking: multiple queries are made, with all matches contained in the sum of these queries
# - No duplication of returned matches
def test_get_cherrypicked_samples_chunking_no_beckman(config):
    # Note: This represents the results of three different (Sentinel, Beckman) sets of
    # database queries, each Sentinel query getting indexed from 0. Do not change the
    # indices here unless you have modified the behaviour of the query.
    query_results = [
        pd.DataFrame(["MCM001"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0]),  # Sentinel query response
        pd.DataFrame([]),  # Beckman query response
        pd.DataFrame(["MCM003"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0]),  # Sentinel query response
        pd.DataFrame([]),  # Beckman query response
        pd.DataFrame(["MCM005"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0]),  # Sentinel query response
        pd.DataFrame([]),  # Beckman query response
    ]
    expected = pd.DataFrame(["MCM001", "MCM003", "MCM005"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0, 1, 2])

    samples = ["MCM001", "MCM002", "MCM003", "MCM004", "MCM005"]
    plate_barcodes = ["123", "456"]

    with patch("sqlalchemy.create_engine", return_value=Mock()):
        with patch("pandas.read_sql", side_effect=query_results):
            returned_samples = get_cherrypicked_samples(config, samples, plate_barcodes, 2)
            pd.testing.assert_frame_equal(expected, returned_samples)


# Test Scenario
# - Actual database responses
# - Only the Sentinel queries return matches (No Beckman)
# - Chunking: multiple queries are made, with all matches contained in the sum of these queries
# - Duplication of returned matches across different chunks: duplicates should be filtered out
def test_get_cherrypicked_samples_repeat_tests_no_beckman(config, mlwh_sentinel_cherrypicked, event_wh_data):
    # the following come from MLWH_SAMPLE_STOCK_RESOURCE in test data
    root_sample_ids = ["root_1", "root_2", "root_3", "root_1"]
    plate_barcodes = ["pb_1", "pb_2", "pb_3"]

    # root_1 will match 2 samples, but only one of those will match an event (on Sanger Sample Id)
    # therefore we only get 1 of the samples called 'root_1' back (the one on plate 'pb_1')
    # this also checks we don't get a duplicate row for root_1 / pb_1, despite it cropped up in 2
    # different 'chunks'
    expected_rows = [
        ["root_1", "pb_1", "positive", "A1"],
        ["root_2", "pb_2", "positive", "A1"],
        ["root_3", "pb_3", "positive", "A1"],
    ]
    expected_columns = [FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE, "Result_lower", FIELD_COORDINATE]
    expected = pd.DataFrame(np.array(expected_rows), columns=expected_columns, index=[0, 1, 2])

    chunk_size = 2
    returned_samples = get_cherrypicked_samples(config, root_sample_ids, plate_barcodes, chunk_size)
    pd.testing.assert_frame_equal(expected, returned_samples)


# Test Scenario
# - Mocking database responses
# - Only the Beckman query returns matches (No Sentinel)
# - No chunking: a single query is made in which all matches are returned
# - No duplication of returned matches
def test_get_cherrypicked_samples_no_sentinel(config):
    expected = [
        # Sentinel query response
        pd.DataFrame([]),
        # Beckman query response
        pd.DataFrame(["MCM001", "MCM003", "MCM005"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0, 1, 2]),
    ]
    samples = ["MCM001", "MCM002", "MCM003", "MCM004", "MCM005"]
    plate_barcodes = ["123", "456"]

    with patch("sqlalchemy.create_engine", return_value=Mock()):
        with patch("pandas.read_sql", side_effect=expected):
            returned_samples = get_cherrypicked_samples(config, samples, plate_barcodes)
            assert returned_samples.at[0, FIELD_ROOT_SAMPLE_ID] == "MCM001"  # type: ignore
            assert returned_samples.at[1, FIELD_ROOT_SAMPLE_ID] == "MCM003"  # type: ignore
            assert returned_samples.at[2, FIELD_ROOT_SAMPLE_ID] == "MCM005"  # type: ignore


# Test Scenario
# - Mocking database responses
# - Only the Beckman queries return matches (No Sentinel)
# - Chunking: multiple queries are made, with all matches contained in the sum of these queries
# - No duplication of returned matches
def test_get_cherrypicked_samples_chunking_no_sentinel(config):
    # Note: This represents the results of three different (Sentinel, Beckman) sets of
    # database queries, each Beckman query getting indexed from 0. Do not change the
    # indices here unless you have modified the behaviour of the query.
    query_results = [
        pd.DataFrame([]),  # Sentinel query response
        pd.DataFrame(["MCM001"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0]),  # Beckman query response
        pd.DataFrame([]),  # Sentinel query response
        pd.DataFrame(["MCM003"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0]),  # Beckman query response
        pd.DataFrame([]),  # Sentinel query response
        pd.DataFrame(["MCM005"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0]),  # Beckman query response
    ]
    expected = pd.DataFrame(["MCM001", "MCM003", "MCM005"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0, 1, 2])

    samples = ["MCM001", "MCM002", "MCM003", "MCM004", "MCM005"]
    plate_barcodes = ["123", "456"]

    with patch("sqlalchemy.create_engine", return_value=Mock()):
        with patch("pandas.read_sql", side_effect=query_results):
            returned_samples = get_cherrypicked_samples(config, samples, plate_barcodes, 2)
            pd.testing.assert_frame_equal(expected, returned_samples)


# Test Scenario
# - Actual database responses
# - Only the Beckman queries return matches (No Sentinel)
# - Chunking: multiple queries are made, with all matches contained in the sum of these queries
# - Duplication of returned matches across different chunks: duplicates should be filtered out
def test_get_cherrypicked_samples_repeat_tests_no_sentinel(config, mlwh_beckman_cherrypicked, event_wh_data):
    # the following come from MLWH_SAMPLE_LIGHTHOUSE_SAMPLE in test data
    root_sample_ids = ["root_5", "root_6", "root_5"]
    plate_barcodes = ["pb_4", "pb_5", "pb_6"]

    # root_5 will match 2 samples, but only one of those will match an event (on sample uuid)
    # therefore we only get 1 of the samples called 'root_5' back (the one on plate 'pb_4')
    # this also checks we don't get a duplicate row for root_5 / pb_4, despite it cropped up in 2
    # different 'chunks'
    expected_rows = [["root_5", "pb_4", "positive", "A1"], ["root_6", "pb_5", "positive", "A1"]]
    expected_columns = [FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE, "Result_lower", FIELD_COORDINATE]
    expected = pd.DataFrame(np.array(expected_rows), columns=expected_columns, index=[0, 1])

    chunk_size = 2
    returned_samples = get_cherrypicked_samples(config, root_sample_ids, plate_barcodes, chunk_size)
    pd.testing.assert_frame_equal(expected, returned_samples)


# Test Scenario
# - Mocking database responses
# - Both Sentinel and Beckman queries return matches
# - No chunking: a single query is made (per workflow) in which all matches are returned
# - Duplication of returned matches across different workflows: duplicates should be filtered out
def test_get_cherrypicked_samples_sentinel_and_beckman(config):
    expected = [
        # Sentinel query response
        pd.DataFrame(["MCM001", "MCM006"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0, 1]),
        # Beckman query response
        pd.DataFrame(["MCM001", "MCM003", "MCM005"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0, 1, 2]),
    ]
    samples = ["MCM001", "MCM002", "MCM003", "MCM004", "MCM005", "MCM006"]
    plate_barcodes = ["123", "456"]

    with patch("sqlalchemy.create_engine", return_value=Mock()):
        with patch("pandas.read_sql", side_effect=expected):
            returned_samples = get_cherrypicked_samples(config, samples, plate_barcodes)
            assert returned_samples.at[0, FIELD_ROOT_SAMPLE_ID] == "MCM001"  # type: ignore
            assert returned_samples.at[1, FIELD_ROOT_SAMPLE_ID] == "MCM006"  # type: ignore
            assert returned_samples.at[2, FIELD_ROOT_SAMPLE_ID] == "MCM003"  # type: ignore
            assert returned_samples.at[3, FIELD_ROOT_SAMPLE_ID] == "MCM005"  # type: ignore


# Test Scenario
# - Mocking database responses
# - Both Sentinel and Beckman queries return matches
# - Chunking: multiple queries are made (per workflow), with all matches contained in the sum
# - Duplication of returned matches across different workflows: duplicates should be filtered out
def test_get_cherrypicked_samples_chunking_sentinel_and_beckman(config):
    # Note: This represents the results of three different (Sentinel, Beckman) sets of
    # database queries, each query getting indexed from 0. Do not change the
    # indices here unless you have modified the behaviour of the query.
    query_results = [
        pd.DataFrame(["MCM001"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0]),  # Sentinel query response
        pd.DataFrame(["MCM001", "MCM002"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0, 1]),  # Beckman query response
        pd.DataFrame(["MCM003"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0]),  # Sentinel query response
        pd.DataFrame(["MCM003", "MCM004"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0, 1]),  # Beckman query response
        pd.DataFrame(["MCM005"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0]),  # Sentinel query response
        pd.DataFrame(["MCM005", "MCM006"], columns=[FIELD_ROOT_SAMPLE_ID], index=[0, 1]),  # Beckman query response
    ]
    expected = pd.DataFrame(
        ["MCM001", "MCM002", "MCM003", "MCM004", "MCM005", "MCM006"],
        columns=[FIELD_ROOT_SAMPLE_ID],
        index=[0, 1, 2, 3, 4, 5],
    )

    samples = ["MCM001", "MCM002", "MCM003", "MCM004", "MCM005"]
    plate_barcodes = ["123", "456"]

    with patch("sqlalchemy.create_engine", return_value=Mock()):
        with patch("pandas.read_sql", side_effect=query_results):
            returned_samples = get_cherrypicked_samples(config, samples, plate_barcodes, 2)
            pd.testing.assert_frame_equal(expected, returned_samples)


# Test Scenario
# - Actual database responses
# - Both Sentinel and Beckman queries return matches
# - Chunking: multiple queries are made, with all matches contained in the sum of these queries
# - Duplication of returned matches across different chunks: duplicates should be filtered out
def test_get_cherrypicked_samples_repeat_tests_sentinel_and_beckman(
    config, mlwh_sentinel_and_beckman_cherrypicked, event_wh_data
):
    # the following come from MLWH_SAMPLE_STOCK_RESOURCE and MLWH_SAMPLE_LIGHTHOUSE_SAMPLE in test data
    root_sample_ids = ["root_1", "root_2", "root_3", "root_4", "root_5", "root_6", "root_1"]
    plate_barcodes = ["pb_1", "pb_3", "pb_4", "pb_5", "pb_6"]

    # root_1 will match 2 samples, but only one of those will match a Sentinel event (on pb_1)
    # root_2 will match a single sample with a matching Sentinel event, but excluded as plate pb_2 not included in query
    # root_3 will match a single sample with a matching Sentinel event (on pb_3)
    # root_4 will match 2 samples, but not match either a Sentinel or Beckman event
    # root_5 will match 2 samples, but only one of those will match a Beckman event (on pb_4)
    # root_6 will match a single sample with a matching Beckman event (on pb_5)
    # We also chunk to further test different scenarios
    expected_rows = [
        ["root_1", "pb_1", "positive", "A1"],
        ["root_3", "pb_3", "positive", "A1"],
        ["root_5", "pb_4", "positive", "A1"],
        ["root_6", "pb_5", "positive", "A1"],
    ]
    expected_columns = [FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE, "Result_lower", FIELD_COORDINATE]
    expected = pd.DataFrame(np.array(expected_rows), columns=expected_columns, index=[0, 1, 2, 3])

    chunk_size = 2
    returned_samples = get_cherrypicked_samples(config, root_sample_ids, plate_barcodes, chunk_size)
    pd.testing.assert_frame_equal(expected, returned_samples)
