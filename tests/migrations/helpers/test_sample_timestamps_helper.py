import datetime

from migrations.helpers.sample_timestamps_helper import CREATED_DATE_FIELD_NAME, add_timestamps_to_samples


def generate_example_samples(range):
    samples = []
    for n in range:
        samples.append(
            {
                "Root Sample ID": "TLS0000000" + str(n),
                "RNA ID": "TL-rna-00000001_A01",
                "Result": "Positive",
                "Date Tested": "2020-05-10 14:01:00 UTC",
                "Lab ID": "TLS",
                "source": "Test Lab Somewhere",
            }
        )

    return samples


def generate_example_samples_fields_null(range):
    samples = []
    for n in range:
        samples.append(
            {
                "Root Sample ID": "TLS0000000" + str(n),
                "RNA ID": "TL-rna-00000001_A01",
                "Result": None,
                "Date Tested": "2020-05-10 14:01:00 UTC",
                "Lab ID": "TLS",
                "source": "Test Lab Somewhere",
            }
        )

    return samples


def generate_example_samples_fields_missing(range):
    samples = []
    for n in range:
        samples.append(
            {
                "Root Sample ID": "TLS0000000" + str(n),
                "RNA ID": "TL-rna-00000001_A01",
                "Date Tested": "2020-05-10 14:01:00 UTC",
                "Lab ID": "TLS",
                "source": "Test Lab Somewhere",
            }
        )

    return samples


def test_basic(mongo_database):
    _, db = mongo_database

    db.samples.insert_many(generate_example_samples(range(0, 6)))

    db.samples_200519_1510.insert_many(generate_example_samples(range(0, 2)))
    db.samples_200520_1510.insert_many(generate_example_samples(range(0, 4)))
    db.samples_21052020_1510.insert_many(generate_example_samples(range(0, 6)))

    add_timestamps_to_samples(db)

    total_samples = db.samples.count_documents({})
    samples_with_timestamp = db.samples.count_documents({CREATED_DATE_FIELD_NAME: {"$ne": None}})
    for sample in db.samples.find():
        print(f"DEBUG: sample: {sample}")

    assert total_samples == samples_with_timestamp

    from_samples_200519_1510 = db.samples.find({"Root Sample ID": {"$in": ["TLS00000000", "TLS00000001"]}})
    for sample in from_samples_200519_1510:
        assert sample[CREATED_DATE_FIELD_NAME] == datetime.datetime(2020, 5, 19, 15, 10)

    from_samples_200520_1510 = db.samples.find({"Root Sample ID": {"$in": ["TLS00000002", "TLS00000003"]}})
    for sample in from_samples_200520_1510:
        assert sample[CREATED_DATE_FIELD_NAME] == datetime.datetime(2020, 5, 20, 15, 10)

    samples_07052020_1610 = db.samples.find({"Root Sample ID": {"$in": ["TLS00000004", "TLS00000005"]}})
    for sample in samples_07052020_1610:
        assert sample[CREATED_DATE_FIELD_NAME] == datetime.datetime(2020, 5, 21, 15, 10)


def test_fields_null(mongo_database):
    _, db = mongo_database

    db.samples.insert_many(generate_example_samples_fields_null(range(0, 2)))
    db.samples_200519_1510.insert_many(generate_example_samples_fields_null(range(0, 2)))

    add_timestamps_to_samples(db)

    samples_with_null_concat_id = db.samples.count_documents({"concat_id": None})
    assert samples_with_null_concat_id == db.samples.count_documents({})

    samples_with_null_concat_id_2 = db.samples_200519_1510.count_documents({"concat_id": None})
    assert samples_with_null_concat_id_2 == db.samples_200519_1510.count_documents({})

    samples_with_timestamp = db.samples.count_documents({CREATED_DATE_FIELD_NAME: {"$ne": None}})
    for sample in db.samples.find():
        print(f"DEBUG: sample: {sample}")

    assert samples_with_timestamp == 0  # shouldn't find any matches to update if concat_ids are null


def test_fields_missing(mongo_database):
    _, db = mongo_database

    db.samples.insert_many(generate_example_samples_fields_missing(range(0, 2)))
    db.samples_200519_1510.insert_many(generate_example_samples_fields_missing(range(0, 2)))

    add_timestamps_to_samples(db)

    samples_with_null_concat_id = db.samples.count_documents({"concat_id": None})
    assert samples_with_null_concat_id == db.samples.count_documents({})

    samples_with_null_concat_id_2 = db.samples_200519_1510.count_documents({"concat_id": None})
    assert samples_with_null_concat_id_2 == db.samples_200519_1510.count_documents({})

    samples_with_timestamp = db.samples.count_documents({CREATED_DATE_FIELD_NAME: {"$ne": None}})
    for sample in db.samples.find():
        print(f"DEBUG: sample: {sample}")

    assert samples_with_timestamp == 0  # shouldn't find any matches to update if concat_ids are null
