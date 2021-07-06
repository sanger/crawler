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
)


@pytest.fixture
def request_post_mock():
    with patch('requests.post') as mock:
        yield mock


def test_flatten_reduces_lists():
    actual = flatten([[1, 2], [3, 4], [5, 6]])
    expected = [1, 2, 3, 4, 5, 6]

    assert actual == expected


def test_flatten_reduces_one_level_only():
    actual = flatten([[1, [2, 3]], [[4, 5], 6]])
    expected = [1, [2, 3], [4, 5], 6]

    assert actual == expected


@pytest.mark.parametrize('count', [2, 3])
def test_generate_baracoda_barcodes_calls_correct_baracoda_endpoint(request_post_mock, count):
    expected = ['TEST-012345', 'TEST-012346', 'TEST-012347']
    request_post_mock.return_value.json.return_value = { "barcodes_group": { "barcodes": expected } }
    actual = generate_baracoda_barcodes(count)

    assert request_post_mock.called_with(f"http://uat.baracoda.psd.sanger.ac.uk/barcodes_group/TEST/new?count={count}")
    assert actual == expected


@pytest.mark.parametrize('count', [2, 3])
def test_create_barcodes(count):
    expected = ['TEST-012345', 'TEST-012346', 'TEST-012347']

    with patch('crawler.helpers.cherrypicker_test_data.generate_baracoda_barcodes') as generate_barcodes:
        generate_barcodes.return_value = expected
        actual = create_barcodes(count)

        assert generate_barcodes.called_with(count)
        assert actual == expected


@pytest.mark.parametrize('barcode, well_num, expected', [
    ['TEST-123450', 4, 'RSID-TEST-1234500004'],
    ['TEST-123451', 34, 'RSID-TEST-1234510034'],
    ['TEST-123452', 96, 'RSID-TEST-1234520096'],
])
def test_create_root_sample_id(barcode, well_num, expected):
    actual = create_root_sample_id(barcode, well_num)
    assert actual == expected


@pytest.mark.parametrize('barcode, well_num, well_coordinate, expected', [
    ['TEST-123450', 4, 'A04', 'VPID-TEST-1234500004_A04'],
    ['TEST-123451', 34, 'C10', 'VPID-TEST-1234510034_C10'],
    ['TEST-123452', 96, 'H12', 'VPID-TEST-1234520096_H12'],
])
def test_create_viral_prep_id(barcode, well_num, well_coordinate, expected):
    actual = create_viral_prep_id(barcode, well_num, well_coordinate)
    assert actual == expected


@pytest.mark.parametrize('barcode, well_coordinate, expected', [
    ['TEST-123450', 'A04', 'TEST-123450_A04'],
    ['TEST-123451', 'C10', 'TEST-123451_C10'],
    ['TEST-123452', 'H12', 'TEST-123452_H12'],
])
def test_create_rna_id(barcode, well_coordinate, expected):
    actual = create_rna_id(barcode, well_coordinate)
    assert actual == expected


@pytest.mark.parametrize('barcode, well_num, well_coordinate, expected', [
    ['TEST-123450', 4, 'A04', 'RNA_PCR-TEST-1234500004_A04'],
    ['TEST-123451', 34, 'C10', 'RNA_PCR-TEST-1234510034_C10'],
    ['TEST-123452', 96, 'H12', 'RNA_PCR-TEST-1234520096_H12'],
])
def test_create_rna_pcr_id(barcode, well_num, well_coordinate, expected):
    actual = create_rna_pcr_id(barcode, well_num, well_coordinate)
    assert actual == expected


def test_create_test_timestamp():
    dt = datetime(2012, 3, 4, 5, 6, 7)
    expected = '2012-03-04 05:06:07 UTC'
    actual = create_test_timestamp(dt)

    assert actual == expected
