import uuid
from datetime import datetime, timedelta
import pytest
from unittest.mock import patch

from crawler.constants import (
    FIELD_BARCODE,
    FIELD_CREATED_AT,
    FIELD_LAB_ID,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_UPDATED_AT,
)
from migrations.helpers.dart_samples_update_helper import (
    add_sample_uuid_field,
    get_positive_samples,
    new_mongo_source_plate,
    remove_cherrypicked_samples,
    update_dart,
)


# ----- update_dart tests -----


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


# ----- other tests -----


def generate_example_samples(range, start_datetime):
    samples = []
    #  create positive samples
    for n in range:
        samples.append(
            {
                FIELD_ROOT_SAMPLE_ID: f"TLS0000000{n}",
                FIELD_RESULT: "Positive",
                FIELD_PLATE_BARCODE: f"DN1000000{n}",
                FIELD_LAB_ID: "TLS",
                FIELD_CREATED_AT: start_datetime + timedelta(days=n),
                FIELD_UPDATED_AT: start_datetime + timedelta(days=n),
            }
        )

    # create negative sample
    samples.append(
        {
            FIELD_ROOT_SAMPLE_ID: "TLS0000000_neg",
            FIELD_RESULT: "Negative",
            FIELD_PLATE_BARCODE: "DN10000000",
            FIELD_LAB_ID: "TLS",
            FIELD_CREATED_AT: start_datetime,
            FIELD_UPDATED_AT: start_datetime,
        }
    )

    # create control sample
    samples.append(
        {
            FIELD_ROOT_SAMPLE_ID: "CBIQA_TLS0000000_control",
            FIELD_RESULT: "Positive",
            FIELD_PLATE_BARCODE: "DN10000000",
            FIELD_LAB_ID: "TLS",
            FIELD_CREATED_AT: start_datetime,
            FIELD_UPDATED_AT: start_datetime,
        }
    )
    return samples


def test_mongo_aggregate(mongo_database):
    _, mongo_db = mongo_database

    start_datetime = datetime(year=2020, month=5, day=10, hour=15, minute=10)

    # generate and insert sample rows into the mongo database
    test_samples = generate_example_samples(range(0, 6), start_datetime)
    mongo_db.samples.insert_many(test_samples)

    assert mongo_db.samples.count_documents({}) == 8

    # although 6 samples would be created, test that we are selecting only a subset using dates
    assert len(get_positive_samples(mongo_db.samples, start_datetime, (start_datetime + timedelta(days=2)))) == 3


def test_add_sample_uuid_field():
    test_samples = generate_example_samples(range(0, 6), datetime.now())

    for sample in add_sample_uuid_field(test_samples):
        assert FIELD_LH_SAMPLE_UUID in [*sample]
        assert type(sample[FIELD_LH_SAMPLE_UUID]) == str


def test_remove_cherrypicked_samples():
    test_samples = generate_example_samples(range(0, 6), datetime.now())
    mock_cherry_picked_sample = [test_samples[0][FIELD_ROOT_SAMPLE_ID], test_samples[0][FIELD_PLATE_BARCODE]]

    samples = remove_cherrypicked_samples(test_samples, [mock_cherry_picked_sample])
    assert len(samples) == 7
    assert mock_cherry_picked_sample[0] not in [sample[FIELD_ROOT_SAMPLE_ID] for sample in samples]


def test_new_mongo_source_plate(freezer):
    now = datetime.now()
    plate_barcode = "PLATE_BARCODE_123"
    lab_id = "LAB_ID_123"
    source_plate = new_mongo_source_plate(plate_barcode, lab_id)

    assert {FIELD_LH_SOURCE_PLATE_UUID, FIELD_BARCODE, FIELD_LAB_ID, FIELD_UPDATED_AT, FIELD_CREATED_AT} == set(
        [*source_plate]
    )
    assert uuid.UUID(str(source_plate[FIELD_LH_SOURCE_PLATE_UUID]))
    assert source_plate[FIELD_BARCODE] == plate_barcode
    assert source_plate[FIELD_LAB_ID] == lab_id
    assert source_plate[FIELD_UPDATED_AT] == now
    assert source_plate[FIELD_CREATED_AT] == now


# ----- update_dart tests -----


def test_update_dart_returns_early_invalid_start_datetime(mock_mongo_client, mock_mysql_connection, mock_update_dart):
    start_datetime = "not a real datetime"
    end_datetime = "201017_1200"
    with patch("migrations.helpers.dart_samples_update_helper.valid_datetime_string", return_value=False):
        update_dart(start_datetime, end_datetime)

        # ensure no database connections/updates are made
        mock_mongo_client.assert_not_called()
        mock_mysql_connection.assert_not_called()
        mock_update_dart.assert_not_called()


def test_update_dart_returns_early_invalid_end_datetime(mock_mongo_client, mock_mysql_connection, mock_update_dart):
    start_datetime = "201016_1600"
    end_datetime = "not a real datetime"
    with patch("migrations.helpers.dart_samples_update_helper.valid_datetime_string", side_effect=[True, False]):
        update_dart(start_datetime, end_datetime)

        # ensure no database connections/updates are made
        mock_mongo_client.assert_not_called()
        mock_mysql_connection.assert_not_called()
        mock_update_dart.assert_not_called()


def test_update_dart_returns_early_start_datetime_after_end_datetime(
    mock_mongo_client, mock_mysql_connection, mock_update_dart
):
    start_datetime = "201017_1200"
    end_datetime = "201016_1600"
    update_dart(start_datetime, end_datetime)

    # ensure no database connections/updates are made
    mock_mongo_client.assert_not_called()
    mock_mysql_connection.assert_not_called()
    mock_update_dart.assert_not_called()
