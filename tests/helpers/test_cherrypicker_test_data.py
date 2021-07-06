import pytest
from unittest.mock import patch

from crawler.helpers.cherrypicker_test_data import (
    flatten,
    generate_baracoda_barcodes,
    create_barcodes,
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
    expected = ['TEST-012345', 'TEST-012346', 'TEST-012347']
    request_post_mock.return_value.json.return_value = { "barcodes_group": { "barcodes": expected } }
    actual = generate_baracoda_barcodes(count)

    assert request_post_mock.called_with(f"http://uat.baracoda.psd.sanger.ac.uk/barcodes_group/TEST/new?count={count}")
    assert actual == expected


@pytest.mark.parametrize("count", [2, 3])
def test_create_barcodes(count):
    expected = ['TEST-012345', 'TEST-012346', 'TEST-012347']

    with patch('crawler.helpers.cherrypicker_test_data.generate_baracoda_barcodes') as generate_barcodes:
        generate_barcodes.return_value = expected
        actual = create_barcodes(count)

        assert generate_barcodes.called_with(count)
        assert actual == expected
