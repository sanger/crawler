import json
import os
import shutil
from collections import namedtuple
from datetime import datetime
from functools import partial
from http import HTTPStatus
from unittest.mock import patch

import pytest
import responses
from requests import ConnectionError

from crawler.exceptions import CherrypickerDataError
from crawler.helpers.cherrypicker_test_data import (
    create_barcode_meta,
    create_barcodes,
    create_csv_rows,
    create_plate_rows,
    create_rna_id,
    create_rna_pcr_id,
    create_root_sample_id,
    create_row,
    create_test_timestamp,
    create_viral_prep_id,
    flat_list_of_positives_per_plate,
    flatten,
    generate_baracoda_barcodes,
    write_plates_file,
)
from crawler.helpers.general_helpers import is_found_in_list

LoggerMessages = namedtuple("LoggerMessages", ["info", "error"])


@pytest.fixture
def logger_messages():
    with patch("crawler.helpers.cherrypicker_test_data.logger") as logger:
        infos = []
        logger.info.side_effect = lambda msg: infos.append(msg)

        errors = []
        logger.error.side_effect = lambda msg: errors.append(msg)

        yield LoggerMessages(info=infos, error=errors)


@pytest.fixture
def request_post_mock():
    with patch("requests.post") as mock:
        yield mock


def test_flatten_reduces_lists():
    actual = flatten([[1, 2], [3, 4], [5, 6]])
    expected = [1, 2, 3, 4, 5, 6]

    assert actual == expected


def test_flatten_reduces_one_level_only():
    actual = flatten([[1, [2, 3]], [[4, 5], 6]])
    expected = [1, [2, 3], [4, 5], 6]

    assert actual == expected


@pytest.mark.parametrize("count", [2, 3])
def test_create_barcodes(config, count):
    expected = ["TEST-012345", "TEST-012346", "TEST-012347"]

    with patch(
        "crawler.helpers.cherrypicker_test_data.generate_baracoda_barcodes", return_value=expected
    ) as generate_barcodes:
        actual = create_barcodes(config, count)

    assert generate_barcodes.called_with(count)
    assert actual == expected


@pytest.fixture
def mocked_responses():
    """Easily mock responses from HTTP calls.
    https://github.com/getsentry/responses#responses-as-a-pytest-fixture"""
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.mark.parametrize("count", [2, 3])
def test_generate_baracoda_barcodes_working_fine(config, count, mocked_responses):
    expected = ["TEST-012345", "TEST-012346", "TEST-012347"]
    baracoda_url = f"{config.BARACODA_BASE_URL}/barcodes_group/TEST/new?count={count}"

    mocked_responses.add(
        responses.POST,
        baracoda_url,
        json={"barcodes_group": {"barcodes": expected}},
        status=HTTPStatus.CREATED,
    )

    out = generate_baracoda_barcodes(config, count)
    assert out == expected
    assert len(mocked_responses.calls) == 1


@pytest.mark.parametrize("count", [2, 3])
def test_generate_baracoda_barcodes_will_retry_if_fail(config, count, mocked_responses):
    baracoda_url = f"{config.BARACODA_BASE_URL}/barcodes_group/TEST/new?count={count}"

    mocked_responses.add(
        responses.POST,
        baracoda_url,
        json={"errors": ["Some error from baracoda"]},
        status=HTTPStatus.INTERNAL_SERVER_ERROR,
    )

    with pytest.raises(Exception):
        generate_baracoda_barcodes(config, count)

    assert len(mocked_responses.calls) == config.BARACODA_RETRY_ATTEMPTS


@pytest.mark.parametrize("count", [2, 3])
@pytest.mark.parametrize("exception_type", [ConnectionError, Exception])
def test_generate_baracoda_barcodes_will_retry_if_exception(config, count, exception_type, mocked_responses):
    baracoda_url = f"{config.BARACODA_BASE_URL}/barcodes_group/TEST/new?count={count}"

    mocked_responses.add(
        responses.POST,
        baracoda_url,
        body=exception_type("Some error"),
        status=HTTPStatus.INTERNAL_SERVER_ERROR,
    )

    with pytest.raises(CherrypickerDataError):
        generate_baracoda_barcodes(config, count)

    assert len(mocked_responses.calls) == config.BARACODA_RETRY_ATTEMPTS


@pytest.mark.parametrize("count", [2, 3])
def test_generate_baracoda_barcodes_will_not_raise_error_if_success_after_retry(config, count, mocked_responses):
    expected = ["TEST-012345", "TEST-012346", "TEST-012347"]
    baracoda_url = f"{config.BARACODA_BASE_URL}/barcodes_group/TEST/new?count={count}"

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

    generate_baracoda_barcodes(config, count)

    assert len(mocked_responses.calls) == config.BARACODA_RETRY_ATTEMPTS


@pytest.mark.parametrize(
    "barcode, well_num, expected",
    [
        ["TEST-123450", 4, "RSID-TEST-12345004"],
        ["TEST-123451", 34, "RSID-TEST-12345134"],
        ["TEST-123452", 96, "RSID-TEST-12345296"],
    ],
)
def test_create_root_sample_id(barcode, well_num, expected):
    actual = create_root_sample_id(barcode, well_num)
    assert actual == expected


@pytest.mark.parametrize(
    "barcode, well_num, well_coordinate, expected",
    [
        ["TEST-123450", 4, "A04", "VPID-TEST-12345004_A04"],
        ["TEST-123451", 34, "C10", "VPID-TEST-12345134_C10"],
        ["TEST-123452", 96, "H12", "VPID-TEST-12345296_H12"],
    ],
)
def test_create_viral_prep_id(barcode, well_num, well_coordinate, expected):
    actual = create_viral_prep_id(barcode, well_num, well_coordinate)
    assert actual == expected


@pytest.mark.parametrize(
    "barcode, well_coordinate, expected",
    [
        ["TEST-123450", "A04", "TEST-123450_A04"],
        ["TEST-123451", "C10", "TEST-123451_C10"],
        ["TEST-123452", "H12", "TEST-123452_H12"],
    ],
)
def test_create_rna_id(barcode, well_coordinate, expected):
    actual = create_rna_id(barcode, well_coordinate)
    assert actual == expected


@pytest.mark.parametrize(
    "barcode, well_num, well_coordinate, expected",
    [
        ["TEST-123450", 4, "A04", "RNA_PCR-TEST-12345004_A04"],
        ["TEST-123451", 34, "C10", "RNA_PCR-TEST-12345134_C10"],
        ["TEST-123452", 96, "H12", "RNA_PCR-TEST-12345296_H12"],
    ],
)
def test_create_rna_pcr_id(barcode, well_num, well_coordinate, expected):
    actual = create_rna_pcr_id(barcode, well_num, well_coordinate)
    assert actual == expected


def test_create_test_timestamp():
    dt = datetime(2012, 3, 4, 5, 6, 7)
    expected = "2012-03-04 05:06:07 UTC"
    actual = create_test_timestamp(dt)

    assert actual == expected


@patch("crawler.helpers.cherrypicker_test_data.create_test_timestamp")
@patch("crawler.helpers.cherrypicker_test_data.create_rna_pcr_id")
@patch("crawler.helpers.cherrypicker_test_data.create_rna_id")
@patch("crawler.helpers.cherrypicker_test_data.create_viral_prep_id")
@patch("crawler.helpers.cherrypicker_test_data.create_root_sample_id")
def test_create_row(rs_id, vp_id, rna_id, rna_pcr_id, timestamp):
    rs_id.return_value = "RSID"
    vp_id.return_value = "VPID"
    rna_id.return_value = "RNAID"
    rna_pcr_id.return_value = "RNAPCRID"
    timestamp.return_value = "TS"

    dt = datetime(2012, 3, 4, 5, 6, 7)
    well_index = 2
    result = "Positive"
    barcode = "TEST-123456"
    lab_id = "TEST-LAB"

    actual = create_row(dt, well_index, result, barcode, lab_id)

    expected = ["RSID", "VPID", "RNAID", "RNAPCRID", result, "TS", lab_id]
    well_num = well_index + 1
    well_coordinate = "A02"

    assert actual == expected
    assert rs_id.called_with(barcode, well_num)
    assert vp_id.called_with(barcode, well_num, well_coordinate)
    assert rna_id.called_with(barcode, well_coordinate)
    assert rna_pcr_id.called_with(barcode, well_num, well_coordinate)
    assert timestamp.called_with(dt)


@patch("random.shuffle")
@patch("crawler.helpers.cherrypicker_test_data.create_row")
def test_create_plate_rows(create_row, shuffle):
    positives = negatives = 0
    dt = datetime(2012, 3, 4, 5, 6, 7)
    barcode = "TEST-123456"
    lab_id = "TEST-LAB"

    def create_row_side_effect(dt_arg, _, result_arg, barcode_arg, lab_id_arg):
        nonlocal positives, negatives, dt, barcode

        assert dt_arg == dt
        assert barcode_arg == barcode
        assert lab_id_arg == lab_id

        if result_arg == "Positive":
            positives += 1
        elif result_arg == "Negative":
            negatives += 1

        return ["A", "row"]

    create_row.side_effect = create_row_side_effect

    actual = create_plate_rows(dt, 40, barcode, lab_id)
    expected = [["A", "row"]] * 96

    assert actual == expected
    assert shuffle.called_with(["Positive"] * 40 + ["Negative"] * 56)
    assert create_row.call_count == 96
    assert positives == 40
    assert negatives == 56


def test_flat_list_of_positives_per_plate():
    actual = flat_list_of_positives_per_plate([[2, 5], [3, 10]])
    expected = [5, 5, 10, 10, 10]

    assert actual == expected


def test_create_csv_rows():
    # Note this is an integration of all the methods tested above, so not strictly a unit test!
    plate_specs = [[1, 0], [2, 40], [1, 96], [2, 40]]
    dt = datetime(2012, 3, 4, 5, 6, 7)
    barcodes = {
        "TEST-00POS01": 0,
        "TEST-40POS01": 40,
        "TEST-40POS02": 40,
        "TEST-96POS01": 96,
        "TEST-40POS03": 40,
        "TEST-40POS04": 40,
    }
    lab_id = "TEST-LAB"

    actual = create_csv_rows(plate_specs, dt, list(barcodes.keys()), lab_id)

    wells_per_plate = 96
    expected_count = wells_per_plate * len(barcodes)
    assert len(actual) == expected_count

    # Check that identifier fields are unique across the rows
    assert len(set([row[0] for row in actual])) == expected_count
    assert len(set([row[1] for row in actual])) == expected_count
    assert len(set([row[2] for row in actual])) == expected_count
    assert len(set([row[3] for row in actual])) == expected_count

    # Check that the timestamp and lab ID was added to all rows identically
    assert len(set([row[5] for row in actual])) == 1
    assert len(set([row[6] for row in actual])) == 1
    assert actual[0][6] == lab_id

    # Per plate checks
    for barcode, positives in barcodes.items():
        barcode_rows = [row for row in actual if barcode in row[0]]
        assert len(barcode_rows) == wells_per_plate

        # Assert that expected fields contain the correct prefix and barcode
        assert all([row[0].startswith(f"RSID-{barcode}") for row in barcode_rows])
        assert all([row[1].startswith(f"VPID-{barcode}") for row in barcode_rows])
        assert all([row[2].startswith(f"{barcode}_") for row in barcode_rows])
        assert all([row[3].startswith(f"RNA_PCR-{barcode}") for row in barcode_rows])

        # Check the correct number of positives and negatives were generated
        positive_rows = [row for row in barcode_rows if "Positive" == row[4]]
        negative_rows = [row for row in barcode_rows if "Negative" == row[4]]
        assert len(positive_rows) == positives
        assert len(negative_rows) == 96 - positives


@pytest.fixture
def test_rows_data():
    return [
        ["RSID-01", "VPID-01", "RNAID-01", "RNAPCRID-01", "Positive", "Timestamp", "CPTD"],
        ["RSID-02", "VPID-02", "RNAID-02", "RNAPCRID-02", "Negative", "Timestamp", "CPTD"],
    ]


@pytest.fixture
def expected_test_output():
    return """Root Sample ID,Viral Prep ID,RNA ID,RNA-PCR ID,Result,Date Tested,Lab ID
RSID-01,VPID-01,RNAID-01,RNAPCRID-01,Positive,Timestamp,CPTD
RSID-02,VPID-02,RNAID-02,RNAPCRID-02,Negative,Timestamp,CPTD
"""  # noqa E501


@pytest.mark.parametrize("existing_output_path", [True, False])
def test_write_plates_file_success(existing_output_path, test_rows_data, expected_test_output, logger_messages):
    data_path = os.path.join("tmp", "data")
    output_path = os.path.join(data_path, "TEST")
    filename = "testing.csv"

    shutil.rmtree(data_path, ignore_errors=True)

    try:
        if existing_output_path:
            os.makedirs(output_path)

        write_plates_file(test_rows_data, output_path, filename)

        with open(os.path.join(output_path, filename), mode="r") as f:
            saved_data = f.read()
        assert saved_data == expected_test_output

    finally:
        shutil.rmtree(data_path, ignore_errors=True)

    assert len(logger_messages.error) == 0
    assert is_found_in_list("Writing to file", logger_messages.info)
    assert is_found_in_list("Test data plates file written", logger_messages.info)
    assert is_found_in_list("testing.csv", logger_messages.info)


def test_write_plates_file_exception(test_rows_data, logger_messages):
    output_path = os.path.join("tmp", "data", "TEST")
    filename = "testing.csv"

    with patch("builtins.open", side_effect=OSError(5, "Unable to write file")):
        with pytest.raises(OSError):
            write_plates_file(test_rows_data, output_path, filename)

    assert is_found_in_list("Exception", logger_messages.error)
    assert is_found_in_list("Unable to write file", logger_messages.error)


def test_create_barcode_meta():
    barcodes = ["TEST-123450", "TEST-123451", "TEST-123452", "TEST-123453", "TEST-123454"]
    actual = create_barcode_meta([[2, 5], [3, 10]], barcodes)
    expected = [
        ["TEST-123450", "number of positives: 5"],
        ["TEST-123451", "number of positives: 5"],
        ["TEST-123452", "number of positives: 10"],
        ["TEST-123453", "number of positives: 10"],
        ["TEST-123454", "number of positives: 10"],
    ]

    assert actual == expected
