import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from crawler.constants import (
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    FIELD_BARCODE,
    FIELD_CREATED_AT,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGO_LAB_ID,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_UPDATED_AT,
)
from migrations.helpers.dart_samples_update_helper import (
    add_sample_uuid_field,
    get_samples,
    migrate_all_dbs,
    new_mongo_source_plate,
    samples_updated_with_source_plate_uuids,
    update_mongo_fields,
)
from tests.conftest import MockedError

# ----- test helpers -----


@pytest.fixture
def mock_mongo_client():
    with patch("migrations.helpers.dart_samples_update_helper.create_mongo_client") as mock_client:
        yield mock_client


@pytest.fixture
def mock_mysql_connection():
    with patch("migrations.helpers.dart_samples_update_helper.create_mysql_connection") as mock_conn:
        yield mock_conn


@pytest.fixture
def mock_update_dart():
    with patch("migrations.helpers.dart_samples_update_helper.update_dart_fields") as mock_update:
        yield mock_update


def generate_example_samples(range, start_datetime):
    samples = []
    #  create positive samples
    for n in range:
        samples.append(
            {
                FIELD_MONGODB_ID: str(uuid.uuid4()),
                FIELD_ROOT_SAMPLE_ID: f"TLS0000000{n}",
                FIELD_RESULT: "Positive",
                FIELD_PLATE_BARCODE: f"DN1000000{n}",
                FIELD_MONGO_LAB_ID: "TLS",
                FIELD_RNA_ID: f"rna_{n}",
                FIELD_CREATED_AT: start_datetime + timedelta(days=n),
                FIELD_UPDATED_AT: start_datetime + timedelta(days=n),
            }
        )

    # create negative sample
    samples.append(
        {
            FIELD_MONGODB_ID: str(uuid.uuid4()),
            FIELD_ROOT_SAMPLE_ID: "TLS0000000_neg",
            FIELD_RESULT: "Negative",
            FIELD_PLATE_BARCODE: "DN20000000",
            FIELD_MONGO_LAB_ID: "TLS",
            FIELD_RNA_ID: "rna_negative",
            FIELD_CREATED_AT: start_datetime,
            FIELD_UPDATED_AT: start_datetime,
        }
    )

    # create control sample
    samples.append(
        {
            FIELD_MONGODB_ID: str(uuid.uuid4()),
            FIELD_ROOT_SAMPLE_ID: "CBIQA_TLS0000000_control",
            FIELD_RESULT: "Positive",
            FIELD_PLATE_BARCODE: "DN30000000",
            FIELD_MONGO_LAB_ID: "TLS",
            FIELD_RNA_ID: "rna_sample",
            FIELD_CREATED_AT: start_datetime,
            FIELD_UPDATED_AT: start_datetime,
        }
    )
    return samples


# ----- helper method tests -----


def test_get_samples(mongo_database):
    _, mongo_db = mongo_database

    start_datetime = datetime(year=2020, month=5, day=10, hour=15, minute=10)

    # generate and insert sample rows into the mongo database
    test_samples = generate_example_samples(range(0, 6), start_datetime)
    mongo_db.samples.insert_many(test_samples)

    assert mongo_db.samples.count_documents({}) == 8

    # although 6 samples would be created, test that we are selecting only a subset using dates, including the -ve one
    assert len(get_samples(mongo_db.samples, start_datetime, (start_datetime + timedelta(days=2)))) == 5


def test_add_sample_uuid_field():
    test_samples = generate_example_samples(range(0, 6), datetime.now())

    for sample in add_sample_uuid_field(test_samples):
        assert FIELD_LH_SAMPLE_UUID in [*sample]
        assert type(sample[FIELD_LH_SAMPLE_UUID]) == str


def test_new_mongo_source_plate(freezer):
    now = datetime.now()
    plate_barcode = "PLATE_BARCODE_123"
    lab_id = "LAB_ID_123"
    source_plate = new_mongo_source_plate(plate_barcode, lab_id)

    assert {FIELD_LH_SOURCE_PLATE_UUID, FIELD_BARCODE, FIELD_MONGO_LAB_ID, FIELD_UPDATED_AT, FIELD_CREATED_AT} == set(
        [*source_plate]
    )
    assert uuid.UUID(str(source_plate[FIELD_LH_SOURCE_PLATE_UUID]))
    assert source_plate[FIELD_BARCODE] == plate_barcode
    assert source_plate[FIELD_MONGO_LAB_ID] == lab_id
    assert source_plate[FIELD_UPDATED_AT] == now
    assert source_plate[FIELD_CREATED_AT] == now


def test_samples_updated_with_source_plate_uuids(mongo_database):
    _, mongo_db = mongo_database

    test_samples = generate_example_samples(range(0, 6), datetime.now())

    updated_samples = samples_updated_with_source_plate_uuids(mongo_db, test_samples)

    assert uuid.UUID(str(updated_samples[0][FIELD_LH_SOURCE_PLATE_UUID]))
    # TODO more rigorously test this:
    # - test samples all have a UUID
    # - test source plates added
    # - test samples refer to source plates


def test_update_mongo_fields(mongo_database):
    _, mongo_db = mongo_database
    test_samples = generate_example_samples(range(0, 6), datetime.now())
    mongo_db[COLLECTION_SAMPLES].insert_many(test_samples)

    idx = 0
    for sample in test_samples:
        sample[FIELD_LH_SAMPLE_UUID] = f"{idx}"
        sample[FIELD_LH_SOURCE_PLATE_UUID] = f"{idx}"
        idx += 1

    assert update_mongo_fields(mongo_db, test_samples)
    for sample in test_samples:
        assert (
            mongo_db[COLLECTION_SAMPLES].count_documents(
                {
                    FIELD_LH_SAMPLE_UUID: sample[FIELD_LH_SAMPLE_UUID],
                    FIELD_LH_SOURCE_PLATE_UUID: sample[FIELD_LH_SOURCE_PLATE_UUID],
                }
            )
            == 1
        )


# ----- migrate_all_dbs tests -----


def test_migrate_all_dbs_returns_early_invalid_start_datetime(
    config, mock_mongo_client, mock_mysql_connection, mock_update_dart
):
    start_datetime = "not a real datetime"
    end_datetime = "201017_1200"
    with patch("migrations.helpers.dart_samples_update_helper.valid_datetime_string", return_value=False):
        migrate_all_dbs(config, start_datetime, end_datetime)

        # ensure no database connections/updates are made
        mock_mongo_client.assert_not_called()
        mock_mysql_connection.assert_not_called()
        mock_update_dart.assert_not_called()


def test_migrate_all_dbs_returns_early_invalid_end_datetime(
    config, mock_mongo_client, mock_mysql_connection, mock_update_dart
):
    start_datetime = "201016_1600"
    end_datetime = "not a real datetime"
    with patch("migrations.helpers.dart_samples_update_helper.valid_datetime_string", side_effect=[True, False]):
        migrate_all_dbs(config, start_datetime, end_datetime)

        # ensure no database connections/updates are made
        mock_mongo_client.assert_not_called()
        mock_mysql_connection.assert_not_called()
        mock_update_dart.assert_not_called()


def test_migrate_all_dbs_returns_early_start_datetime_after_end_datetime(
    config, mock_mongo_client, mock_mysql_connection, mock_update_dart
):
    start_datetime = "201017_1200"
    end_datetime = "201016_1600"
    migrate_all_dbs(config, start_datetime, end_datetime)

    # ensure no database connections/updates are made
    mock_mongo_client.assert_not_called()
    mock_mysql_connection.assert_not_called()
    mock_update_dart.assert_not_called()


def test_migrate_all_dbs_return_early_no_samples(config, mock_mysql_connection, mock_update_dart):
    start_datetime = datetime(year=2020, month=5, day=10, hour=15, minute=10)
    end_datetime = start_datetime + timedelta(days=1)

    mock_samples_collection = MagicMock()
    mock_samples_collection.aggregate.return_value = []  # mock samples collection call
    mock_source_plates_collection = MagicMock()

    def side_effect(_, collection_name):
        if collection_name == COLLECTION_SAMPLES:
            return mock_samples_collection
        elif collection_name == COLLECTION_SOURCE_PLATES:
            return mock_source_plates_collection
        else:
            raise ValueError(f"{collection_name} is not recognized/expected")

    with patch("migrations.helpers.dart_samples_update_helper.get_mongo_collection", side_effect=side_effect):
        migrate_all_dbs(config, start_datetime.strftime("%y%m%d_%H%M"), end_datetime.strftime("%y%m%d_%H%M"))

        # ensure no updates are made
        mock_samples_collection.bulk_write.assert_not_called()
        mock_source_plates_collection.insert_many.assert_not_called()
        mock_mysql_connection.assert_not_called()
        mock_update_dart.assert_not_called()


def test_migrate_all_dbs_returns_early_failed_mongo_samples_update(config, mock_mysql_connection, mock_update_dart):
    start_datetime = datetime(year=2020, month=5, day=10, hour=15, minute=10)
    end_datetime = start_datetime + timedelta(days=1)
    test_samples = generate_example_samples(range(0, 6), start_datetime)

    def side_effect(_, collection_name):
        mock_collection = MagicMock()
        if collection_name == COLLECTION_SAMPLES:
            mock_collection.aggregate.return_value = test_samples
            mock_collection.bulk_write.side_effect = MockedError("Boom!")
        elif collection_name == COLLECTION_SOURCE_PLATES:
            mock_collection.find_one.return_value = None
        else:
            raise ValueError(f"{collection_name} is not recognized/expected")
        return mock_collection

    with patch("migrations.helpers.dart_samples_update_helper.get_mongo_collection", side_effect=side_effect):
        migrate_all_dbs(config, start_datetime.strftime("%y%m%d_%H%M"), end_datetime.strftime("%y%m%d_%H%M"))

        mock_mysql_connection.assert_not_called()
        mock_update_dart.assert_not_called()


def test_migrate_all_dbs_returns_early_failed_mongo_source_plates_update(
    config, mock_mysql_connection, mock_update_dart
):
    start_datetime = datetime(year=2020, month=5, day=10, hour=15, minute=10)
    end_datetime = start_datetime + timedelta(days=1)
    test_samples = generate_example_samples(range(0, 6), start_datetime)

    def side_effect(_, collection_name):
        mock_collection = MagicMock()
        if collection_name == COLLECTION_SAMPLES:
            mock_collection.aggregate.return_value = test_samples
        elif collection_name == COLLECTION_SOURCE_PLATES:
            mock_collection.find_one.return_value = None
            mock_collection.insert_many.side_effect = MockedError("Boom!")
        else:
            raise ValueError(f"{collection_name} is not recognized/expected")
        return mock_collection

    with patch("migrations.helpers.dart_samples_update_helper.get_mongo_collection", side_effect=side_effect):
        migrate_all_dbs(config, start_datetime.strftime("%y%m%d_%H%M"), end_datetime.strftime("%y%m%d_%H%M"))

        mock_mysql_connection.assert_not_called()
        mock_update_dart.assert_not_called()


def test_migrate_all_dbs_returns_early_failed_mlwh_update(config, mock_mysql_connection, mock_update_dart):
    start_datetime = datetime(year=2020, month=5, day=10, hour=15, minute=10)
    end_datetime = start_datetime + timedelta(days=1)
    test_samples = generate_example_samples(range(0, 6), start_datetime)

    def side_effect(_, collection_name):
        mock_collection = MagicMock()
        if collection_name == COLLECTION_SAMPLES:
            mock_collection.aggregate.return_value = test_samples
        elif collection_name == COLLECTION_SOURCE_PLATES:
            mock_collection.find_one.return_value = None
        else:
            raise ValueError(f"{collection_name} is not recognized/expected")
        return mock_collection

    with patch("migrations.helpers.dart_samples_update_helper.get_mongo_collection", side_effect=side_effect):
        mock_mysql_connection.side_effect = MockedError("Boom!")  # mock creating MLWH mysql connection to fail

        migrate_all_dbs(config, start_datetime.strftime("%y%m%d_%H%M"), end_datetime.strftime("%y%m%d_%H%M"))

        mock_update_dart.assert_not_called()


def test_migrate_all_dbs_with_cherry_picked_samples_mocked(
    config, mongo_database, mock_mysql_connection, mock_update_dart
):
    _, mongo_db = mongo_database

    start_datetime = datetime(year=2020, month=5, day=10, hour=15, minute=10)
    end_datetime = start_datetime + timedelta(days=1)
    # generate and insert sample rows into the mongo database
    test_samples = generate_example_samples(range(0, 6), start_datetime)
    mongo_db.samples.insert_many(test_samples)

    assert mongo_db.samples.count_documents({}) == 8

    cherry_picked_sample = {
        FIELD_ROOT_SAMPLE_ID: [test_samples[0][FIELD_ROOT_SAMPLE_ID]],
        FIELD_PLATE_BARCODE: [test_samples[0][FIELD_PLATE_BARCODE]],
    }

    cherry_picked_df = pd.DataFrame.from_dict(cherry_picked_sample)

    with patch(
        "migrations.helpers.dart_samples_update_helper.get_cherrypicked_samples", side_effect=[cherry_picked_df]
    ):
        with patch(
            "migrations.helpers.dart_samples_update_helper.remove_cherrypicked_samples"
        ) as mock_remove_cherrypicked_samples:

            migrate_all_dbs(config, start_datetime.strftime("%y%m%d_%H%M"), end_datetime.strftime("%y%m%d_%H%M"))

            samples = get_samples(mongo_db.samples, start_datetime, end_datetime)

            mock_remove_cherrypicked_samples.assert_called_once_with(
                samples, [[cherry_picked_sample[FIELD_ROOT_SAMPLE_ID][0], cherry_picked_sample[FIELD_PLATE_BARCODE][0]]]
            )


def test_migrate_all_dbs_remove_cherrypicked_samples(config, mongo_database, mock_update_dart):
    _, mongo_db = mongo_database

    start_datetime = datetime(year=2020, month=5, day=10, hour=15, minute=10)
    end_datetime = start_datetime + timedelta(days=1)
    # generate and insert sample rows into the mongo database
    test_samples = generate_example_samples(range(0, 6), start_datetime)
    mongo_db.samples.insert_many(test_samples)

    assert mongo_db.samples.count_documents({}) == 8

    with patch(
        "migrations.helpers.dart_samples_update_helper.remove_cherrypicked_samples"
    ) as mock_remove_cherrypicked_samples:
        migrate_all_dbs(config, start_datetime.strftime("%y%m%d_%H%M"), end_datetime.strftime("%y%m%d_%H%M"))

        mock_remove_cherrypicked_samples.assert_not_called()
