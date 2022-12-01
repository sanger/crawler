from datetime import datetime, timedelta
from unittest.mock import call, patch

import pytest
from bson import ObjectId

from crawler.constants import (
    FIELD_COORDINATE,
    FIELD_LH_SAMPLE_UUID,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_UPDATED_AT,
)
from migrations import back_populate_uuids_plate_barcodes as subject

CSV_FILEPATH = "./tests/data/populate_old_plates_1.csv"

MONGO_SAMPLE_WITHOUT_UUID = {
    FIELD_ROOT_SAMPLE_ID: "R00T-001",
    FIELD_COORDINATE: "A01",
    FIELD_PLATE_BARCODE: "123",
    FIELD_MONGODB_ID: ObjectId("aaaaaaaaaaaaaaaaaaaaa001"),
}

MONGO_SAMPLE_WITH_EMPTY_STRING_UUID = {
    FIELD_ROOT_SAMPLE_ID: "R00T-001",
    FIELD_COORDINATE: "A01",
    FIELD_PLATE_BARCODE: "123",
    FIELD_MONGODB_ID: ObjectId("aaaaaaaaaaaaaaaaaaaaa001"),
    FIELD_LH_SAMPLE_UUID: "",
}

MONGO_SAMPLE_WITH_EXISTING_UUID = {
    FIELD_ROOT_SAMPLE_ID: "R00T-001",
    FIELD_COORDINATE: "A01",
    FIELD_PLATE_BARCODE: "123",
    FIELD_MONGODB_ID: ObjectId("aaaaaaaaaaaaaaaaaaaaa001"),
    FIELD_LH_SAMPLE_UUID: "Existing-UUID",
}

MATCHING_MLWH_LIGHTHOUSE_SAMPLE = {
    "lighthouse_sample": [
        {
            "mongodb_id": "aaaaaaaaaaaaaaaaaaaaa001",
            "root_sample_id": "R00T-001",
            "cog_uk_id": "123ABC",
            "rna_id": "123_A01",
            "plate_barcode": "123",
            "coordinate": "A1",
            "result": "Positive",
            "date_tested_string": "2020-10-24 22:30:22",
            "date_tested": datetime(2020, 10, 24, 22, 30, 22),
            "source": "test centre",
            "lab_id": "TC",
            "lh_sample_uuid": "123-UUID",
        },
    ]
}

NON_MATCHING_MLWH_LIGHTHOUSE_SAMPLE = {
    "lighthouse_sample": [
        {
            "mongodb_id": "aaaaaaaaaaaaaaaaaaaaa999",
            "root_sample_id": "R00T-001",
            "cog_uk_id": "123ABC",
            "rna_id": "123_A01",
            "plate_barcode": "123",
            "coordinate": "A1",
            "result": "Positive",
            "date_tested_string": "2020-10-24 22:30:22",
            "date_tested": datetime(2020, 10, 24, 22, 30, 22),
            "source": "test centre",
            "lab_id": "TC",
            "lh_sample_uuid": "123-UUID",
        },
    ]
}


@pytest.fixture
def logger():
    with patch("migrations.back_populate_uuids_plate_barcodes.LOGGER") as logger:
        yield logger


def test_run_handles_no_samples_in_mongo(config):
    subject.run(config, CSV_FILEPATH)


@pytest.mark.parametrize("samples_collection_accessor", [[MONGO_SAMPLE_WITHOUT_UUID]], indirect=True)
@pytest.mark.parametrize("mlwh_sql_engine", [MATCHING_MLWH_LIGHTHOUSE_SAMPLE], indirect=True)
def test_run_updates_uuid_in_mongo_correctly(config, samples_collection_accessor, mlwh_sql_engine, freezer, logger):
    subject.run(config, CSV_FILEPATH)

    assert samples_collection_accessor.count_documents({}) == 1

    sample = samples_collection_accessor.find({})[0]
    assert sample[FIELD_LH_SAMPLE_UUID] == "123-UUID"
    assert sample["uuid_updated"] is True

    # Note that MongoDB rounds the milliseconds, hence this check being a less-than operation
    assert datetime.utcnow() - sample[FIELD_UPDATED_AT] < timedelta(seconds=1)

    logger.info.assert_has_calls(
        [call("Count of successful Mongo updates = 1"), call("Count of failed Mongo updates = 0")]
    )


@pytest.mark.parametrize("samples_collection_accessor", [[MONGO_SAMPLE_WITH_EMPTY_STRING_UUID]], indirect=True)
@pytest.mark.parametrize("mlwh_sql_engine", [MATCHING_MLWH_LIGHTHOUSE_SAMPLE], indirect=True)
def test_run_updates_uuid_in_mongo_when_currently_empty_string(config, samples_collection_accessor, mlwh_sql_engine):
    subject.run(config, CSV_FILEPATH)

    assert samples_collection_accessor.count_documents({}) == 1

    sample = samples_collection_accessor.find({})[0]
    assert sample["lh_sample_uuid"] == "123-UUID"
    assert sample["uuid_updated"] is True


@pytest.mark.parametrize("samples_collection_accessor", [[MONGO_SAMPLE_WITH_EXISTING_UUID]], indirect=True)
@pytest.mark.parametrize("mlwh_sql_engine", [MATCHING_MLWH_LIGHTHOUSE_SAMPLE], indirect=True)
def test_run_throws_exception_when_uuid_already_exists(config, samples_collection_accessor, mlwh_sql_engine):
    with pytest.raises(subject.ExceptionSampleWithSampleUUID) as exc_info:
        subject.run(config, CSV_FILEPATH)

    assert "aaaaaaaaaaaaaaaaaaaaa001" in str(exc_info)

    assert samples_collection_accessor.count_documents({}) == 1

    sample = samples_collection_accessor.find({})[0]
    assert sample["lh_sample_uuid"] == "Existing-UUID"
    assert "uuid_update" not in sample


@pytest.mark.parametrize("samples_collection_accessor", [[MONGO_SAMPLE_WITHOUT_UUID]], indirect=True)
def test_run_throws_exception_when_no_samples_in_mlwh(config, samples_collection_accessor):
    with pytest.raises(subject.ExceptionSampleCountsForMongoAndMLWHNotMatching) as exc_info:
        subject.run(config, CSV_FILEPATH)

    assert "Mongo (1)" in str(exc_info)
    assert "MLWH (0)" in str(exc_info)

    assert samples_collection_accessor.count_documents({}) == 1

    sample = samples_collection_accessor.find({})[0]
    assert "lh_sample_uuid" not in sample
    assert "uuid_update" not in sample


@pytest.mark.parametrize("samples_collection_accessor", [[MONGO_SAMPLE_WITHOUT_UUID]], indirect=True)
@pytest.mark.parametrize("mlwh_sql_engine", [NON_MATCHING_MLWH_LIGHTHOUSE_SAMPLE], indirect=True)
def test_run_throws_exception_when_matching_sample_not_in_mlwh(config, samples_collection_accessor, mlwh_sql_engine):
    with pytest.raises(subject.ExceptionSampleCountsForMongoAndMLWHNotMatching) as exc_info:
        subject.run(config, CSV_FILEPATH)

    assert "Mongo (1)" in str(exc_info)
    assert "MLWH (0)" in str(exc_info)

    assert samples_collection_accessor.count_documents({}) == 1

    sample = samples_collection_accessor.find({})[0]
    assert "lh_sample_uuid" not in sample
    assert "uuid_update" not in sample
