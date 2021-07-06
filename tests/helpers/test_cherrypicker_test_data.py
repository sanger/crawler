from datetime import datetime
import pytest
from unittest.mock import patch

from crawler.helpers.cherrypicker_test_data import (
    flatten,
    generate_baracoda_barcodes,
    create_barcodes,
    create_root_sample_id,
    create_viral_prep_id,
    create_rna_id,
    create_rna_pcr_id,
    create_test_timestamp,
    create_row,
    create_plate_rows,
    flat_list_of_positives_per_plate,
    create_csv_rows,
)


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
def test_generate_baracoda_barcodes_calls_correct_baracoda_endpoint(request_post_mock, count):
    expected = ["TEST-012345", "TEST-012346", "TEST-012347"]
    request_post_mock.return_value.json.return_value = {"barcodes_group": {"barcodes": expected}}
    actual = generate_baracoda_barcodes(count)

    assert request_post_mock.called_with(f"http://uat.baracoda.psd.sanger.ac.uk/barcodes_group/TEST/new?count={count}")
    assert actual == expected


@pytest.mark.parametrize("count", [2, 3])
def test_create_barcodes(count):
    expected = ["TEST-012345", "TEST-012346", "TEST-012347"]

    with patch("crawler.helpers.cherrypicker_test_data.generate_baracoda_barcodes") as generate_barcodes:
        generate_barcodes.return_value = expected
        actual = create_barcodes(count)

        assert generate_barcodes.called_with(count)
        assert actual == expected


@pytest.mark.parametrize(
    "barcode, well_num, expected",
    [
        ["TEST-123450", 4, "RSID-TEST-1234500004"],
        ["TEST-123451", 34, "RSID-TEST-1234510034"],
        ["TEST-123452", 96, "RSID-TEST-1234520096"],
    ],
)
def test_create_root_sample_id(barcode, well_num, expected):
    actual = create_root_sample_id(barcode, well_num)
    assert actual == expected


@pytest.mark.parametrize(
    "barcode, well_num, well_coordinate, expected",
    [
        ["TEST-123450", 4, "A04", "VPID-TEST-1234500004_A04"],
        ["TEST-123451", 34, "C10", "VPID-TEST-1234510034_C10"],
        ["TEST-123452", 96, "H12", "VPID-TEST-1234520096_H12"],
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
        ["TEST-123450", 4, "A04", "RNA_PCR-TEST-1234500004_A04"],
        ["TEST-123451", 34, "C10", "RNA_PCR-TEST-1234510034_C10"],
        ["TEST-123452", 96, "H12", "RNA_PCR-TEST-1234520096_H12"],
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

    actual = create_row(dt, well_index, result, barcode)

    expected = ["RSID", "VPID", "RNAID", "RNAPCRID", result, "TS", "AP"] + [""] * 12
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

    def create_row_side_effect(dt_arg, _, result_arg, barcode_arg):
        nonlocal positives, negatives, dt, barcode

        assert dt_arg == dt
        assert barcode_arg == barcode

        if result_arg == "Positive":
            positives += 1
        elif result_arg == "Negative":
            negatives += 1

        return ["A", "row"]

    create_row.side_effect = create_row_side_effect

    actual = create_plate_rows(dt, 40, barcode)
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
    plate_specs = [[1, 0], [2, 40], [1, 96]]
    dt = datetime(2012, 3, 4, 5, 6, 7)
    barcodes = ["TEST-00POS01", "TEST-40POS01", "TEST-40POS02", "TEST-96POS01"]

    actual = create_csv_rows(plate_specs, dt, barcodes)

    assert len(actual) == 96 * len(barcodes)
    for barcode in barcodes:

        assert len([row for row in actual if barcode in row[0]]) == 96
