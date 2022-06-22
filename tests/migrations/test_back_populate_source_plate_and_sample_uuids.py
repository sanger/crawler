from unittest.mock import patch

import pytest
from pymongo import ASCENDING

from crawler.constants import FIELD_RNA_ID
from migrations import back_populate_source_plate_and_sample_uuids
from migrations.back_populate_source_plate_and_sample_uuids import check_samples_are_valid, extract_barcodes

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
    config,
    testing_samples_with_lab_id,
    samples_collection_accessor,
    query_lighthouse_sample,
    mlwh_samples_with_lab_id_for_migration,
):
    filepath = "./tests/data/populate_old_plates.csv"
    samples_before = list(samples_collection_accessor.find({"plate_barcode": "123"}))

    assert len(samples_before) > 0
    for sample in samples_before:
        assert not ("lh_sample_uuid" in sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(samples_collection_accessor.find({"plate_barcode": "123"}))

    viewed_uuids = []
    assert len(samples_after) > 0
    assert len(samples_after) == len(samples_before)
    for sample in samples_after:
        assert sample["lh_sample_uuid"] is not None
        assert not sample["lh_sample_uuid"] in viewed_uuids
        viewed_uuids.append(sample["lh_sample_uuid"])

    # Now we check in mlwh
    cursor = query_lighthouse_sample.execute(
        "SELECT COUNT(*) FROM lighthouse_sample WHERE plate_barcode = '123' AND lh_sample_uuid IS NOT NULL"
    )

    sample_count = cursor.fetchone()[0]
    assert sample_count == len(samples_after)

    cursor = query_lighthouse_sample.execute(
        "SELECT * FROM lighthouse_sample WHERE plate_barcode = '123' AND lh_sample_uuid IS NOT NULL"
    )

    obtained_mlwh_samples = list(cursor.fetchall())
    mongo_dict = {}
    for sample in samples_after:
        mongo_dict[str(sample["_id"])] = sample["lh_sample_uuid"]
    mlwh_dict = {}
    for mlsample in obtained_mlwh_samples:
        mlwh_dict[mlsample["mongodb_id"]] = mlsample["lh_sample_uuid"]

    for mongo_id in mongo_dict.keys():
        assert mongo_dict[mongo_id] == mlwh_dict[mongo_id]


def test_back_populate_source_plate_uuid_and_sample_uuid_works_with_two_plates(
    config, testing_samples_with_lab_id, samples_collection_accessor
):
    filepath = "./tests/data/populate_old_plates_2_plates.csv"
    samples_before = list(
        samples_collection_accessor.find({"plate_barcode": {"$in": ["123", "456"]}}).sort(FIELD_RNA_ID, ASCENDING)
    )

    assert len(samples_before) > 0
    for sample in samples_before:
        assert not ("lh_sample_uuid" in sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(
        samples_collection_accessor.find({"plate_barcode": {"$in": ["123", "456"]}}).sort(FIELD_RNA_ID, ASCENDING)
    )

    viewed_uuids = []
    assert len(samples_after) > 0
    assert len(samples_after) == len(samples_before)
    for sample in samples_after:
        assert sample["lh_sample_uuid"] is not None
        assert not sample["lh_sample_uuid"] in viewed_uuids
        viewed_uuids.append(sample["lh_sample_uuid"])


def test_back_populate_source_plate_uuid_and_sample_uuid_has_source_plate_uuid(
    config,
    testing_samples_with_lab_id,
    samples_collection_accessor,
    query_lighthouse_sample,
    mlwh_samples_with_lab_id_for_migration,
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

    # Now we check in mlwh
    cursor = query_lighthouse_sample.execute(
        "SELECT COUNT(*) FROM lighthouse_sample WHERE lh_source_plate_uuid IS NOT NULL"
    )

    sample_count = cursor.fetchone()[0]
    assert sample_count == len(samples_after)

    cursor = query_lighthouse_sample.execute(
        "SELECT * FROM lighthouse_sample WHERE lh_source_plate_uuid IS NOT NULL ORDER BY mongodb_id ASC"
    )

    obtained_mlwh_samples = list(cursor.fetchall())
    mongo_dict = {}
    for sample in samples_after:
        mongo_dict[str(sample["_id"])] = sample["lh_source_plate_uuid"]
    mlwh_dict = {}
    for mlsample in obtained_mlwh_samples:
        mlwh_dict[mlsample["mongodb_id"]] = mlsample["lh_source_plate_uuid"]

    for mongo_id in mongo_dict.keys():
        assert mongo_dict[mongo_id] == mlwh_dict[mongo_id]


def test_back_populate_source_plate_uuid_and_sample_uuid_has_source_plate_uuid_with_two_plates_input(
    config, testing_samples_with_lab_id, samples_collection_accessor
):
    filepath = "./tests/data/populate_old_plates_2_plates.csv"
    samples_before = list(
        samples_collection_accessor.find({"plate_barcode": {"$in": ["123", "456"]}}).sort(FIELD_RNA_ID, ASCENDING)
    )

    assert len(samples_before) > 0
    for sample in samples_before:
        assert not ("lh_source_plate_uuid" in sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(
        samples_collection_accessor.find({"plate_barcode": {"$in": ["123", "456"]}}).sort(FIELD_RNA_ID, ASCENDING)
    )

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


def test_check_samples_are_valid_finds_problems_with_samples(
    config,
    testing_samples_with_lab_id,
    samples_collection_accessor,
    source_plates_collection_accessor,
    testing_source_plates,
):
    samples_before = list(
        samples_collection_accessor.find({"plate_barcode": {"$in": ["123", "456"]}}).sort(FIELD_RNA_ID, ASCENDING)
    )

    assert len(samples_before) > 0
    for sample in samples_before:
        assert not ("lh_sample_uuid" in sample)

    # When both are right
    try:
        check_samples_are_valid(samples_collection_accessor, source_plates_collection_accessor, ["123", "456"])
    except Exception as exc:
        raise AssertionError(exc)

    # When sample_uuid has value but source_plate_uuid has not
    with pytest.raises(Exception):
        check_samples_are_valid(samples_collection_accessor, source_plates_collection_accessor, ["789"])

    # When source plate uuid has value but sample_uuid has not
    with pytest.raises(Exception):
        check_samples_are_valid(samples_collection_accessor, source_plates_collection_accessor, ["781"])

    # When a sample has a plate not in source plate collection
    with pytest.raises(Exception):
        check_samples_are_valid(samples_collection_accessor, source_plates_collection_accessor, ["782"])

    # When a source plate from input is already defined in the source plates collection
    with pytest.raises(Exception):
        check_samples_are_valid(samples_collection_accessor, source_plates_collection_accessor, ["783"])
