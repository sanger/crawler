from datetime import datetime
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from crawler.constants import FIELD_LH_SAMPLE_UUID
from crawler.helpers.general_helpers import get_config
from migrations import back_populate_source_plate_and_sample_uuids
from migrations.back_populate_source_plate_and_sample_uuids import extract_barcodes

# ----- test fixture helpers -----


@pytest.fixture
def mock_helper_imports():
    with patch("migrations.update_filtered_positives.pending_plate_barcodes_from_dart") as mock_get_plate_barcodes:
        with patch(
            "migrations.update_filtered_positives.positive_result_samples_from_mongo"
        ) as mock_get_positive_samples:
            yield mock_get_plate_barcodes, mock_get_positive_samples


# TODO needs a test barcodes file

# TODO need test samples mongo collection containing samples for plates in barcodes list in test file

# TODO need MLWH lighthouse_samples table containing samples for plates in barcodes list in test file


def test_back_populate_source_plate_uuid_and_sample_uuid_missing_file(config):
    filepath = "not_found"
    with pytest.raises(Exception):
        back_populate_source_plate_and_sample_uuids.run(config, filepath)


def test_back_populate_source_plate_uuid_and_sample_uuid_not_raise_exception(
    config, testing_samples_with_lab_id, samples_collection_accessor
):
    filepath = "./tests/data/populate_old_plates.csv"
    try:
        back_populate_source_plate_and_sample_uuids.run(config, filepath)
    except Exception as exc:
        assert False, f"back_populate raised { exc }"


def test_back_populate_source_plate_uuid_and_sample_uuid_populates_sample_uuid(
    config, testing_samples_with_lab_id, samples_collection_accessor
):
    filepath = "./tests/data/populate_old_plates.csv"
    samples_before = list(samples_collection_accessor.find({"plate_barcode": "123"}))

    assert len(samples_before) > 0
    assert not ("lh_sample_uuid" in samples_before[0])
    assert not ("lh_sample_uuid" in samples_before[1])

    # It should not populate samples_before[2] because that one has already a sample_uuid
    assert "lh_sample_uuid" in samples_before[2]
    unchanged_uuid = samples_before[2]["lh_sample_uuid"]

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(samples_collection_accessor.find({"plate_barcode": "123"}))

    viewed_uuids = []
    assert len(samples_after) > 0
    assert len(samples_after) == len(samples_before)
    for sample in samples_after:
        assert sample["lh_sample_uuid"] is not None
        assert not sample["lh_sample_uuid"] in viewed_uuids
        viewed_uuids.append(sample["lh_sample_uuid"])

    assert unchanged_uuid in viewed_uuids
    assert unchanged_uuid == samples_after[2]["lh_sample_uuid"]


def test_back_populate_source_plate_uuid_and_sample_uuid_has_source_plate_uuid(
    config, testing_samples_with_lab_id, samples_collection_accessor
):
    filepath = "./tests/data/populate_old_plates.csv"
    samples_before = list(samples_collection_accessor.find({"plate_barcode": "123"}))

    assert len(samples_before) > 0
    for sample in samples_before:
        assert not ("lh_source_plate_uuid" in sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(samples_collection_accessor.find({"plate_barcode": "123"}))

    assert len(samples_after) == len(samples_before)
    source_plate_uuid = samples_after[0]["lh_source_plate_uuid"]
    for sample in samples_after:
        assert sample["lh_source_plate_uuid"] is not None
        assert sample["lh_source_plate_uuid"] == source_plate_uuid


def test_back_populate_source_plate_uuid_and_sample_uuid_dont_change_source_plate_other_barcodes(
    config, testing_samples_with_lab_id, samples_collection_accessor
):
    filepath = "./tests/data/populate_old_plates.csv"
    samples_before = list(samples_collection_accessor.find({"plate_barcode": "456"}))

    assert len(samples_before) > 0
    for sample in samples_before:
        assert not ("lh_source_plate_uuid" in sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(samples_collection_accessor.find({"plate_barcode": "456"}))

    assert len(samples_after) == len(samples_before)
    assert len(samples_before) > 0
    for sample in samples_before:
        assert not ("lh_source_plate_uuid" in sample)


def test_back_populate_source_plate_uuid_and_sample_uuid_dont_change_sample_uuid_other_barcodes(
    config, testing_samples_with_lab_id, samples_collection_accessor
):
    filepath = "./tests/data/populate_old_plates.csv"
    samples_before = list(samples_collection_accessor.find({"plate_barcode": "456"}))

    assert len(samples_before) > 0
    for sample in samples_before:
        assert not ("lh_sample_uuid" in sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(samples_collection_accessor.find({"plate_barcode": "456"}))

    assert len(samples_after) == len(samples_before)
    assert len(samples_before) > 0
    for sample in samples_before:
        assert not ("lh_sample_uuid" in sample)


def test_extract_barcodes_read_barcodes(config):
    filepath = "./tests/data/populate_old_plates.csv"

    assert extract_barcodes(config, filepath) == ["123"]
