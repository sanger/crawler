import json
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
    create_rna_id,
    create_root_sample_id,
    flat_list_of_positives_per_plate,
    flatten,
    generate_baracoda_barcodes,
)


@pytest.fixture
def logger():
    with patch("crawler.helpers.cherrypicker_test_data.LOGGER") as logger:
        yield logger


@pytest.fixture
def request_post_mock():
    with patch("requests.post") as mock:
        yield mock


@pytest.fixture
def mocked_responses():
    """Easily mock responses from HTTP calls.
    https://github.com/getsentry/responses#responses-as-a-pytest-fixture"""
    with responses.RequestsMock() as rsps:
        yield rsps


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


def test_flat_list_of_positives_per_plate():
    actual = flat_list_of_positives_per_plate([[2, 5], [3, 10]])
    expected = [5, 5, 10, 10, 10]

    assert actual == expected


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
