from unittest.mock import patch

import pytest

from migrations import back_populate_source_plate_and_sample_uuids
from migrations.back_populate_source_plate_and_sample_uuids import \
    extract_barcodes

# ----- test fixture helpers -----


@pytest.fixture
def mock_helper_imports():
    with patch("migrations.update_filtered_positives.pending_plate_barcodes_from_dart") as mock_get_plate_barcodes:
        with patch(
            "migrations.update_filtered_positives.positive_result_samples_from_mongo"
        ) as mock_get_positive_samples:
            yield mock_get_plate_barcodes, mock_get_positive_samples


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
        raise AssertionError(exc)


def test_back_populate_source_plate_uuid_and_sample_uuid_populates_sample_uuid(
    config, testing_samples_with_lab_id, samples_collection_accessor, query_lighthouse_sample,
    mlwh_samples_with_lab_id_for_migration
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

    # Now we check in mlwh
    cursor = query_lighthouse_sample.execute("SELECT COUNT(*) FROM lighthouse_sample WHERE lh_sample_uuid IS NOT NULL")

    sample_count = cursor.fetchone()[0]
    assert sample_count == len(samples_after)

    cursor = query_lighthouse_sample.execute("SELECT * FROM lighthouse_sample WHERE lh_sample_uuid IS NOT NULL")
    obtained_mlwh_samples = cursor.fetchall()
    pos = 0
    for sample in samples_after:
        assert str(sample['_id']) == obtained_mlwh_samples[pos]['mongodb_id']
        assert sample['lh_sample_uuid'] == obtained_mlwh_samples[pos]['lh_sample_uuid']
        pos = pos + 1



def test_back_populate_source_plate_uuid_and_sample_uuid_works_with_two_plates(
    config, testing_samples_with_lab_id, samples_collection_accessor
):
    filepath = "./tests/data/populate_old_plates_2_plates.csv"
    samples_before = list(samples_collection_accessor.find())

    assert len(samples_before) > 0
    assert not ("lh_sample_uuid" in samples_before[0])
    assert not ("lh_sample_uuid" in samples_before[1])
    assert not ("lh_sample_uuid" in samples_before[2])

    # It should not populate samples_before[2] because that one has already a sample_uuid
    assert "lh_sample_uuid" in samples_before[3]
    unchanged_uuid = samples_before[3]["lh_sample_uuid"]

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(samples_collection_accessor.find())

    viewed_uuids = []
    assert len(samples_after) > 0
    assert len(samples_after) == len(samples_before)
    for sample in samples_after:
        assert sample["lh_sample_uuid"] is not None
        assert not sample["lh_sample_uuid"] in viewed_uuids
        viewed_uuids.append(sample["lh_sample_uuid"])

    assert unchanged_uuid in viewed_uuids
    assert unchanged_uuid == samples_after[3]["lh_sample_uuid"]


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

def test_back_populate_source_plate_uuid_and_sample_uuid_has_source_plate_uuid_with_two_plates_input(
    config, testing_samples_with_lab_id, samples_collection_accessor
):
    filepath = "./tests/data/populate_old_plates_2_plates.csv"
    samples_before = list(samples_collection_accessor.find())

    assert len(samples_before) > 0
    for sample in samples_before:
        assert not ("lh_source_plate_uuid" in sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(samples_collection_accessor.find())

    assert len(samples_after) == len(samples_before)
    source_plate_uuid = samples_after[0]["lh_source_plate_uuid"]
    source_plate_uuid_second = samples_after[1]["lh_source_plate_uuid"]
    assert source_plate_uuid is not None
    assert source_plate_uuid_second is not None
    assert samples_after[0]["lh_source_plate_uuid"] == source_plate_uuid
    assert samples_after[1]["lh_source_plate_uuid"] == source_plate_uuid_second
    assert samples_after[2]["lh_source_plate_uuid"] == source_plate_uuid
    assert samples_after[3]["lh_source_plate_uuid"] == source_plate_uuid


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
