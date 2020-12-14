import datetime
import re

import pymongo
from migrations.helpers.shared_helper import print_exception

CREATED_DATE_FIELD_NAME = "created_at"  # TODO: check with E & A
BATCH_SIZE = 250000
COLLECTION_NAME_FORMAT_1 = r"^samples_(20[\d]{4}_[\d]{4})$"  # e.g. samples_200519_1510
COLLECTION_NAME_FORMAT_2 = r"^samples_([\d]{4}2020_[\d]{4})$"  # e.g. samples_21052020_1510


def add_timestamps_to_samples(db):
    print(f"Time start: {datetime.datetime.now()}")

    try:
        print("\n-- Update samples collection with new concatenated id column --")
        # For updates, '$set' syntax is important, otherwise it overwrites the whole document!
        update_result_1 = db.samples.update_many(
            {},
            [{"$set": {"concat_id": {"$concat": ["$Root Sample ID", " - ", "$RNA ID", " - ", "$Result"]}}}],
        )
        print(f"Time after adding field to samples collection: {datetime.datetime.now()}")
        print("Number samples modified: ", update_result_1.modified_count)

        print("\n-- Add an index for the new concatenated id column --")
        db.samples.create_index([("concat_id", pymongo.ASCENDING)])
        print(f"Time after adding index to samples collection: {datetime.datetime.now()}")

        print("\n-- Order the collections chronologically (oldest first) --")
        # so that when we loop through, the timestamp we set is when the sample first appeared in a
        # collection
        collection_name_to_timestamp = map_collection_name_to_timestamp(db)
        collection_names_chrono_order = sorted(collection_name_to_timestamp)
        # collection_names_chrono_order = ['samples_200804_1430'] # for testing specific files only
        print(f"Time after ordering collections: {datetime.datetime.now()}")
        print(f"Collections in chronological order: {collection_names_chrono_order}")

        for collection_name in collection_names_chrono_order:
            print(f"\n-- Starting processing collection: {collection_name} at {datetime.datetime.now()} --")

            print(f"\n-- Update collection {collection_name} with new concatenated id column --")
            update_result_2 = db[collection_name].update_many(
                {},
                [{"$set": {"concat_id": {"$concat": ["$Root Sample ID", " - ", "$RNA ID", " - ", "$Result"]}}}],
            )
            print(f"Time after adding field to collection: {datetime.datetime.now()}")
            print("Number samples modified: ", update_result_2.modified_count)

            print("\n-- Retrieve all concatenated ids --")
            concat_ids = []
            concat_ids_result = db[collection_name].find({"concat_id": {"$ne": None}}, {"concat_id": True})
            for sample in concat_ids_result:
                concat_ids.append(sample["concat_id"])

            print(f"Time after querying field and building list: {datetime.datetime.now()}")
            print(f"Total number of samples in collection: {db[collection_name].count_documents({})}")
            print(f"Of which, number to process: {len(concat_ids)}")

            print(
                "\n-- Update samples collection with timestamps, where concat id matches and not "
                f"yet migrated, in batches of {BATCH_SIZE} --"
            )
            # in batches, because otherwise get "Error: 'update' command document too large"

            num_batches = len(concat_ids) // BATCH_SIZE
            if len(concat_ids) % BATCH_SIZE != 0:
                num_batches += 1
            print(f"Number of batches: {num_batches}")

            for batch in range(num_batches):
                # Each batch takes ~30 secs for batch size of 250,000
                print(f"\n-- Starting batch {batch + 1} of {num_batches}: {datetime.datetime.now()} -")

                start = batch * BATCH_SIZE
                end = start + BATCH_SIZE
                # this will go out of bounds on the final batch but python is ok with that and
                # just returns the remaining items
                print(f"Processing samples {start} to {end}")
                concat_ids_subset = concat_ids[start:end]  # e.g. 0:250000, 250000:500000 etc.

                update_result_3 = db.samples.update_many(
                    {"concat_id": {"$in": concat_ids_subset}, "migrated_temp": None},
                    {
                        "$set": {
                            CREATED_DATE_FIELD_NAME: collection_name_to_timestamp[collection_name],
                            "migrated_temp": "Yes",
                        }
                    },
                )
                # for testing with static string
                # update_result_3 = db.samples.update_many(
                #   { 'concat_id': { '$in': concat_ids_subset } },
                #   { '$set': { CREATED_DATE_FIELD_NAME: '2020-08-06 14:06 test' } }
                # )
                print(f"Time after querying based on new field and updating with timestamp: {datetime.datetime.now()}")
                print("Number samples modified: ", update_result_3.modified_count)
    except Exception:
        print_exception()


def map_collection_name_to_timestamp(db):
    collection_name_to_timestamp = {}
    for collection_name in db.list_collection_names():
        if is_sample_archive_collection(collection_name):
            timestamp = extract_timestamp(collection_name)
            collection_name_to_timestamp[collection_name] = timestamp

    return collection_name_to_timestamp


def is_sample_archive_collection(collection_name):
    return bool(re.search(COLLECTION_NAME_FORMAT_1, collection_name)) or bool(
        re.search(COLLECTION_NAME_FORMAT_2, collection_name)
    )


def extract_timestamp(collection_name):
    m = re.match(COLLECTION_NAME_FORMAT_1, collection_name)  # e.g. samples_200519_1510

    if m:
        timestamp_string = m.group(1)
        timestamp_datetime = datetime.datetime.strptime(timestamp_string, "%y%m%d_%H%M")
    else:
        m = re.match(COLLECTION_NAME_FORMAT_2, collection_name)  # e.g. samples_21052020_1510

        if m:
            timestamp_string = m.group(1)
            timestamp_datetime = datetime.datetime.strptime(timestamp_string, "%d%m%Y_%H%M")
        else:
            # shouldn't get here, because we check against the regexes in
            # map_collection_name_to_timestamp
            return None

    return timestamp_datetime
