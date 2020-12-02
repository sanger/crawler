import uuid
from datetime import datetime, timedelta

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
    MONGO_DATETIME_FORMAT,
)
from migrations.helpers.dart_samples_update_helper import (
    add_sample_uuid_field,
    get_positive_samples,
    new_mongo_source_plate,
    remove_cherrypicked_samples,
)


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
    mock_cherry_picked_id = test_samples[0][FIELD_ROOT_SAMPLE_ID]

    samples = remove_cherrypicked_samples(test_samples, [mock_cherry_picked_id])
    assert len(samples) == 7
    assert mock_cherry_picked_id not in [sample[FIELD_ROOT_SAMPLE_ID] for sample in samples]


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
