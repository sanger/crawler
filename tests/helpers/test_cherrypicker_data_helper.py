from datetime import datetime
from unittest.mock import patch

import pytest

from crawler.helpers.cherrypicker_test_data import (
    WELL_COORDS,
    _create_rna_id,
    _create_root_sample_id,
    _flat_list_of_positives_per_plate,
    _flatten,
    create_barcode_meta,
    create_barcodes,
    create_plate_messages,
)


@pytest.fixture
def logger():
    with patch("crawler.helpers.cherrypicker_test_data.LOGGER") as logger:
        yield logger


def test_flatten_reduces_lists():
    actual = _flatten([[1, 2], [3, 4], [5, 6]])
    expected = [1, 2, 3, 4, 5, 6]

    assert actual == expected


def test_flatten_reduces_one_level_only():
    actual = _flatten([[1, [2, 3]], [[4, 5], 6]])
    expected = [1, [2, 3], [4, 5], 6]

    assert actual == expected


@pytest.mark.parametrize("count", [2, 3])
def test_create_barcodes(config, count):
    expected = ["TEST-012345", "TEST-012346", "TEST-012347"]

    with patch(
        "crawler.helpers.cherrypicker_test_data.generate_baracoda_barcodes", return_value=expected
    ) as generate_barcodes:
        actual = create_barcodes(config, count)

    generate_barcodes.assert_called_with(config, count)
    assert actual == expected


@pytest.mark.parametrize(
    "barcode, well_num, expected",
    [
        ["TEST-123450", 4, "RSID-TEST-12345004"],
        ["TEST-123451", 34, "RSID-TEST-12345134"],
        ["TEST-123452", 96, "RSID-TEST-12345296"],
    ],
)
def test_create_root_sample_id(barcode, well_num, expected):
    actual = _create_root_sample_id(barcode, well_num)
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
    actual = _create_rna_id(barcode, well_coordinate)
    assert actual == expected


def test_flat_list_of_positives_per_plate():
    actual = _flat_list_of_positives_per_plate([[2, 5], [3, 10]])
    expected = [5, 5, 10, 10, 10]

    assert actual == expected


def test_create_plate_messages():
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

    actual = create_plate_messages(plate_specs, dt, list(barcodes.keys()))

    assert len(actual) == 6

    for message_i in range(6):
        expected_barcode = list(barcodes.keys())[message_i]

        message = actual[message_i]
        assert type(message["messageUuid"]) is bytes
        assert len(message["messageUuid"]) == 36
        assert message["messageCreateDateUtc"] == dt

        plate = message["plate"]
        assert plate["labId"] == "CPTD"
        assert plate["plateBarcode"] == expected_barcode
        assert len(plate["samples"]) == 96

        for sample_i in range(96):
            sample = plate["samples"][sample_i]
            assert type(sample["sampleUuid"]) is bytes
            assert len(sample["sampleUuid"]) == 36
            assert sample["rootSampleId"] == f"RSID-{expected_barcode}{str(sample_i + 1).zfill(2)}"
            assert sample["rnaId"] == f"{expected_barcode}_{WELL_COORDS[sample_i]}"
            assert sample["cogUkId"] == f"{expected_barcode}{hex(sample_i + 1)[2:].zfill(2)}"
            assert sample["plateCoordinate"] == WELL_COORDS[sample_i]
            assert sample["preferentiallySequence"] is False
            assert sample["mustSequence"] is False
            assert sample["fitToPick"] is (True if sample["result"] == "positive" else False)
            assert sample["testedDateUtc"] == dt

        # Check the correct number of positives exist among all the samples
        assert (
            len(list(filter(lambda sample: sample["result"] == "positive", plate["samples"])))
            == barcodes[expected_barcode]
        )


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
