from unittest.mock import patch

import pytest
from pymongo import ASCENDING
from sqlalchemy import text

from crawler.constants import (
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RNA_ID,
    MLWH_MONGODB_ID,
)
from migrations import back_populate_source_plate_and_sample_uuids
from migrations.back_populate_source_plate_and_sample_uuids import (
    ExceptionSampleWithSampleUUIDNotSourceUUID,
    ExceptionSampleWithSourceUUIDNotSampleUUID,
    ExceptionSourcePlateDefined,
    check_samples_are_valid,
    mlwh_count_samples_from_mongo_ids,
)
from tests.testing_objects import TESTING_SAMPLES_WITH_LAB_ID

# ----- test fixture helpers -----


@pytest.fixture
def mock_helper_imports():
    with patch("migrations.update_filtered_positives.pending_plate_barcodes_from_dart") as mock_get_plate_barcodes:
        with patch(
            "migrations.update_filtered_positives.positive_result_samples_from_mongo"
        ) as mock_get_positive_samples:
            yield mock_get_plate_barcodes, mock_get_positive_samples


def check_sample_not_contains_sample_uuid(sample):
    assert (FIELD_LH_SAMPLE_UUID not in sample) or (sample[FIELD_LH_SAMPLE_UUID] is None)


def check_sample_not_contains_source_plate_uuid(sample):
    assert (FIELD_LH_SOURCE_PLATE_UUID not in sample) or (sample[FIELD_LH_SOURCE_PLATE_UUID] is None)


def test_back_populate_source_plate_uuid_and_sample_uuid_missing_file(config):
    filepath = "not_found"
    with pytest.raises(FileNotFoundError):
        back_populate_source_plate_and_sample_uuids.run(config, filepath)


@pytest.mark.parametrize("samples_collection_accessor", [TESTING_SAMPLES_WITH_LAB_ID], indirect=True)
def test_back_populate_source_plate_uuid_and_sample_uuid_not_raise_exception(
    config, samples_collection_accessor, mlwh_samples_with_lab_id_for_migration
):
    filepath = "./tests/data/populate_old_plates_1.csv"

    # Expect no exceptions to be raised -- if any occur the test will fail
    back_populate_source_plate_and_sample_uuids.run(config, filepath)


@pytest.mark.parametrize("samples_collection_accessor", [TESTING_SAMPLES_WITH_LAB_ID], indirect=True)
def test_back_populate_source_plate_uuid_and_sample_uuid_populates_sample_uuid(
    config,
    samples_collection_accessor,
    query_lighthouse_sample,
    mlwh_samples_with_lab_id_for_migration,
):
    filepath = "./tests/data/populate_old_plates_1.csv"
    samples_before = list(samples_collection_accessor.find({FIELD_PLATE_BARCODE: "123"}))

    assert len(samples_before) > 0
    for sample in samples_before:
        check_sample_not_contains_sample_uuid(sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(samples_collection_accessor.find({FIELD_PLATE_BARCODE: "123"}))

    viewed_uuids = []
    assert len(samples_after) > 0
    assert len(samples_after) == len(samples_before)
    for sample in samples_after:
        assert sample[FIELD_LH_SAMPLE_UUID] is not None
        assert not sample[FIELD_LH_SAMPLE_UUID] in viewed_uuids
        viewed_uuids.append(sample[FIELD_LH_SAMPLE_UUID])

    # Now we check in mlwh
    cursor_result = query_lighthouse_sample.execute(
        text("SELECT COUNT(*) FROM lighthouse_sample WHERE plate_barcode = '123' AND lh_sample_uuid IS NOT NULL")
    )

    sample_count = cursor_result.first()[0]
    assert sample_count == len(samples_after)

    cursor_result = query_lighthouse_sample.execute(
        text("SELECT * FROM lighthouse_sample WHERE plate_barcode = '123' AND lh_sample_uuid IS NOT NULL")
    )

    results = list(cursor_result.mappings())
    mongo_dict = {}
    for sample in samples_after:
        mongo_dict[str(sample[FIELD_MONGODB_ID])] = sample[FIELD_LH_SAMPLE_UUID]
    mlwh_dict = {}
    for mlsample in results:
        mlwh_dict[mlsample[MLWH_MONGODB_ID]] = mlsample[FIELD_LH_SAMPLE_UUID]

    for mongo_id in mongo_dict.keys():
        assert mongo_dict[mongo_id] == mlwh_dict[mongo_id]


@pytest.mark.parametrize("samples_collection_accessor", [TESTING_SAMPLES_WITH_LAB_ID], indirect=True)
def test_back_populate_source_plate_uuid_and_sample_uuid_works_with_two_plates(
    config, samples_collection_accessor, mlwh_samples_with_lab_id_for_migration
):
    filepath = "./tests/data/populate_old_plates_2.csv"
    samples_before = list(
        samples_collection_accessor.find({FIELD_PLATE_BARCODE: {"$in": ["123", "456"]}}).sort(FIELD_RNA_ID, ASCENDING)
    )

    assert len(samples_before) > 0
    for sample in samples_before:
        check_sample_not_contains_sample_uuid(sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(
        samples_collection_accessor.find({FIELD_PLATE_BARCODE: {"$in": ["123", "456"]}}).sort(FIELD_RNA_ID, ASCENDING)
    )

    viewed_uuids = []
    assert len(samples_after) > 0
    assert len(samples_after) == len(samples_before)
    for sample in samples_after:
        assert sample[FIELD_LH_SAMPLE_UUID] is not None
        assert not sample[FIELD_LH_SAMPLE_UUID] in viewed_uuids
        viewed_uuids.append(sample[FIELD_LH_SAMPLE_UUID])


@pytest.mark.parametrize("samples_collection_accessor", [TESTING_SAMPLES_WITH_LAB_ID], indirect=True)
def test_back_populate_source_plate_uuid_and_sample_uuid_has_source_plate_uuid(
    config,
    samples_collection_accessor,
    query_lighthouse_sample,
    mlwh_samples_with_lab_id_for_migration,
):
    filepath = "./tests/data/populate_old_plates_1.csv"
    samples_before = list(samples_collection_accessor.find({FIELD_PLATE_BARCODE: "123"}))

    assert len(samples_before) > 0
    for sample in samples_before:
        check_sample_not_contains_source_plate_uuid(sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(samples_collection_accessor.find({FIELD_PLATE_BARCODE: "123"}))

    assert len(samples_after) == len(samples_before)
    source_plate_uuid = samples_after[0][FIELD_LH_SOURCE_PLATE_UUID]
    for sample in samples_after:
        assert sample[FIELD_LH_SOURCE_PLATE_UUID] is not None
        assert sample[FIELD_LH_SOURCE_PLATE_UUID] == source_plate_uuid

    # Now we check in mlwh
    cursor_result = query_lighthouse_sample.execute(
        text("SELECT COUNT(*) FROM lighthouse_sample WHERE lh_source_plate_uuid IS NOT NULL")
    )

    sample_count = cursor_result.first()[0]
    assert sample_count == len(samples_after)

    cursor_result = query_lighthouse_sample.execute(
        text("SELECT * FROM lighthouse_sample WHERE lh_source_plate_uuid IS NOT NULL ORDER BY mongodb_id ASC")
    )

    results = list(cursor_result.mappings())
    mongo_dict = {}
    for sample in samples_after:
        mongo_dict[str(sample[FIELD_MONGODB_ID])] = sample[FIELD_LH_SOURCE_PLATE_UUID]
    mlwh_dict = {}
    for mlsample in results:
        mlwh_dict[mlsample[MLWH_MONGODB_ID]] = mlsample[FIELD_LH_SOURCE_PLATE_UUID]

    for mongo_id in mongo_dict.keys():
        assert mongo_dict[mongo_id] == mlwh_dict[mongo_id]


@pytest.mark.parametrize("samples_collection_accessor", [TESTING_SAMPLES_WITH_LAB_ID], indirect=True)
def test_back_populate_source_plate_uuid_and_sample_uuid_has_source_plate_uuid_with_two_plates_input(
    config, samples_collection_accessor, mlwh_samples_with_lab_id_for_migration
):
    filepath = "./tests/data/populate_old_plates_2.csv"
    samples_before = list(
        samples_collection_accessor.find({FIELD_PLATE_BARCODE: {"$in": ["123", "456"]}}).sort(FIELD_RNA_ID, ASCENDING)
    )

    assert len(samples_before) > 0
    for sample in samples_before:
        check_sample_not_contains_source_plate_uuid(sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(
        samples_collection_accessor.find({FIELD_PLATE_BARCODE: {"$in": ["123", "456"]}}).sort(FIELD_RNA_ID, ASCENDING)
    )

    assert len(samples_after) == len(samples_before)
    source_plate_uuid = samples_after[0][FIELD_LH_SOURCE_PLATE_UUID]
    source_plate_uuid_second = samples_after[3][FIELD_LH_SOURCE_PLATE_UUID]
    assert source_plate_uuid != source_plate_uuid_second
    assert source_plate_uuid is not None
    assert source_plate_uuid_second is not None
    assert samples_after[0][FIELD_LH_SOURCE_PLATE_UUID] == source_plate_uuid
    assert samples_after[1][FIELD_LH_SOURCE_PLATE_UUID] == source_plate_uuid
    assert samples_after[2][FIELD_LH_SOURCE_PLATE_UUID] == source_plate_uuid
    assert samples_after[3][FIELD_LH_SOURCE_PLATE_UUID] == source_plate_uuid_second


@pytest.mark.parametrize("samples_collection_accessor", [TESTING_SAMPLES_WITH_LAB_ID], indirect=True)
def test_back_populate_source_plate_uuid_and_sample_uuid_dont_change_source_plate_other_barcodes(
    config, samples_collection_accessor, mlwh_samples_with_lab_id_for_migration
):
    filepath = "./tests/data/populate_old_plates_1.csv"
    samples_before = list(samples_collection_accessor.find({FIELD_PLATE_BARCODE: "456"}))

    assert len(samples_before) > 0
    for sample in samples_before:
        check_sample_not_contains_source_plate_uuid(sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(samples_collection_accessor.find({FIELD_PLATE_BARCODE: "456"}))

    assert len(samples_after) == len(samples_before)
    assert len(samples_before) > 0
    for sample in samples_after:
        assert not (FIELD_LH_SOURCE_PLATE_UUID in sample)


@pytest.mark.parametrize("samples_collection_accessor", [TESTING_SAMPLES_WITH_LAB_ID], indirect=True)
def test_back_populate_source_plate_uuid_and_sample_uuid_dont_change_sample_uuid_other_barcodes(
    config, samples_collection_accessor, mlwh_samples_with_lab_id_for_migration
):
    filepath = "./tests/data/populate_old_plates_1.csv"
    samples_before = list(samples_collection_accessor.find({FIELD_PLATE_BARCODE: "456"}))

    assert len(samples_before) > 0
    for sample in samples_before:
        check_sample_not_contains_sample_uuid(sample)

    back_populate_source_plate_and_sample_uuids.run(config, filepath)
    samples_after = list(samples_collection_accessor.find({FIELD_PLATE_BARCODE: "456"}))

    assert len(samples_after) == len(samples_before)
    assert len(samples_before) > 0
    for sample in samples_after:
        check_sample_not_contains_sample_uuid(sample)


@pytest.mark.parametrize("samples_collection_accessor", [TESTING_SAMPLES_WITH_LAB_ID], indirect=True)
def test_check_samples_are_valid_finds_problems_with_samples(
    monkeypatch,
    mlwh_connection,
    samples_collection_accessor,
    source_plates_collection_accessor,
    testing_source_plates,
    mlwh_samples_with_lab_id_for_migration,
):
    samples_before = list(
        samples_collection_accessor.find({FIELD_PLATE_BARCODE: {"$in": ["123", "456"]}}).sort(FIELD_RNA_ID, ASCENDING)
    )

    assert len(samples_before) > 0
    for sample in samples_before:
        check_sample_not_contains_sample_uuid(sample)

    # When both are right
    # Expect no exceptions to be raised -- if any occur the test will fail
    check_samples_are_valid(
        mlwh_connection, samples_collection_accessor, source_plates_collection_accessor, ["123", "456"]
    )

    # When sample_uuid has value but source_plate_uuid has not
    with pytest.raises(ExceptionSampleWithSampleUUIDNotSourceUUID):
        check_samples_are_valid(
            mlwh_connection, samples_collection_accessor, source_plates_collection_accessor, ["789"]
        )

    # When sample_uuid has value but source_plate_uuid has not, but the environment variable was set to ignore this
    # Expect no exceptions to be raised -- if any occur the test will fail
    with monkeypatch.context() as mp:
        mp.setenv("SUPPRESS_ERROR_FOR_EXISTING_SAMPLE_UUIDS", "true")
        check_samples_are_valid(
            mlwh_connection, samples_collection_accessor, source_plates_collection_accessor, ["789"]
        )

    # When sample_uuid has value but source_plate_uuid has not, but the suppress environment variable was set to false
    with monkeypatch.context() as mp:
        mp.setenv("SUPPRESS_ERROR_FOR_EXISTING_SAMPLE_UUIDS", "false")
        with pytest.raises(ExceptionSampleWithSampleUUIDNotSourceUUID):
            check_samples_are_valid(
                mlwh_connection, samples_collection_accessor, source_plates_collection_accessor, ["789"]
            )

    # When source plate uuid has value but sample_uuid has not and there is no source plate record
    with pytest.raises(ExceptionSampleWithSourceUUIDNotSampleUUID):
        check_samples_are_valid(
            mlwh_connection, samples_collection_accessor, source_plates_collection_accessor, ["781"]
        )

    # When a sample has sample uuid and source plate and the plate not in source plate collection, is right
    # Expect no exceptions to be raised -- if any occur the test will fail
    check_samples_are_valid(mlwh_connection, samples_collection_accessor, source_plates_collection_accessor, ["782"])

    # When a source plate from input is already defined in the source plates collection
    with pytest.raises(ExceptionSourcePlateDefined):
        check_samples_are_valid(
            mlwh_connection, samples_collection_accessor, source_plates_collection_accessor, ["783"]
        )


def test_count_samples_from_mongo_ids(mlwh_connection, mlwh_samples_with_lab_id_for_migration):
    value = mlwh_count_samples_from_mongo_ids(
        mlwh_connection,
        [
            "aaaaaaaaaaaaaaaaaaaaaaa1",
            "aaaaaaaaaaaaaaaaaaaaaaa2",
            "aaaaaaaaaaaaaaaaaaaaaaa3",
            "aaaaaaaaaaaaaaaaaaaaaaa4",
        ],
    )
    assert value == 4

    value = mlwh_count_samples_from_mongo_ids(
        mlwh_connection,
        [
            "aaaaaaaaaaaaaaaaaaaaaaa1",
            "aaaaaaaaaaaaaaaaaaaaaaa4",
            "aaaaaaaaaaaaaaaaaaaaaaa5",
        ],
    )
    assert value == 3

    value = mlwh_count_samples_from_mongo_ids(mlwh_connection, ["aaaaaaaaaaaaaaaaaaaaaaa6"])
    assert value == 1

    value = mlwh_count_samples_from_mongo_ids(mlwh_connection, ["ASDFASDFASF"])
    assert value == 0
