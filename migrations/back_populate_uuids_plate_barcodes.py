import logging
import logging.config
from datetime import datetime, timezone
from typing import List

from pymongo.collection import Collection

from crawler.constants import (
    COLLECTION_SAMPLES,
    FIELD_COORDINATE,
    FIELD_LH_SAMPLE_UUID,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_UPDATED_AT,
    MLWH_LH_SAMPLE_UUID,
    MLWH_MONGODB_ID,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.sql_queries import SQL_MLWH_COUNT_MONGO_IDS, SQL_MLWH_GET_SAMPLE_FOR_MONGO_ID
from crawler.types import Config, SampleDoc
from migrations.helpers.shared_helper import (
    extract_barcodes,
    extract_mongodb_ids,
    get_mongo_samples_for_source_plate,
    mysql_generator_from_config,
    validate_args,
)

LOGGER = logging.getLogger(__name__)
DATA_LOGGER = logging.getLogger("migration_data")

FIELD_UUID_UPDATED = "uuid_updated"


class ExceptionSampleWithSampleUUID(Exception):
    pass


class ExceptionNoSamplesForGivenPlateBarcodes(Exception):
    pass


class ExceptionSampleCountsForMongoAndMLWHNotMatching(Exception):
    pass


"""
Iterates over the list of plate barcodes provided in a CSV file.
Finds all the samples in MongoDB with that plate barcode and iterates over those.

Looks for the same sample in the lighthouse_sample table using mongodb_id and checks whether it has an lh_sample_uuid.
If not, the sample is skipped.
Otherwise the lh_sample_uuid is added to the MongoDB document along with a key uuid_updated set to 'true'.

Note that newer samples, since Rabbit MQ was used for adding plate maps, will not have a mongodb_id value populated!
If samples are included in the migration where the mongodb_id does not exist in MLWH, an exception will be raised.
"""


def run(config: Config, s_filepath: str) -> None:
    filepath = validate_args(config=config, s_filepath=s_filepath)

    LOGGER.info("-" * 80)
    LOGGER.info("STARTING BACK POPULATING UUIDS")
    LOGGER.info(f"Time start: {datetime.now()}")

    source_plate_barcodes = extract_barcodes(filepath=filepath)

    LOGGER.info(f"Starting update process with input file {filepath}")

    update_mongo_uuids(config=config, source_plate_barcodes=source_plate_barcodes)


def update_mongo_uuids(config: Config, source_plate_barcodes: List[str]) -> None:
    """Updates source plate and sample uuids in both mongo and mlwh

    Arguments:
        config {Config} -- application config specifying database details
        source_plate_barcodes {List[str]} -- the list of source plate barcodes

    Returns:
        Nothing
    """
    # counters to track progress
    counter_mongo_update_successes = 0
    counter_mongo_update_failures = 0

    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        check_samples_are_valid(config, samples_collection, source_plate_barcodes)

        for source_plate_barcode in source_plate_barcodes:
            LOGGER.info(f"Processing source plate barcode {source_plate_barcode}")

            # List[SampleDoc]
            sample_docs = get_mongo_samples_for_source_plate(samples_collection, source_plate_barcode)

            # iterate through samples
            for sample_doc in sample_docs:
                # will every sample doc have a plate_barcode and lab id?
                LOGGER.info(f"Sample in well {sample_doc[FIELD_COORDINATE]}")

                # update sample in Mongo ‘samples’ to set lh_sample_uuid and updated_timestamp
                try:
                    query = SQL_MLWH_GET_SAMPLE_FOR_MONGO_ID % {MLWH_MONGODB_ID: sample_doc[FIELD_MONGODB_ID]}
                    mlwh_sample = next(mysql_generator_from_config(config=config, query=query))

                    if mlwh_sample[MLWH_LH_SAMPLE_UUID] is None:
                        continue

                    log_mongo_sample_fields("Before update", sample_doc)

                    sample_doc[FIELD_LH_SAMPLE_UUID] = mlwh_sample[MLWH_LH_SAMPLE_UUID]
                    sample_doc[FIELD_UUID_UPDATED] = True
                    sample_doc[FIELD_UPDATED_AT] = datetime.now(tz=timezone.utc)

                    success = update_mongo_sample(samples_collection, sample_doc)
                    if success:
                        log_mongo_sample_fields("After successful update", sample_doc)
                        counter_mongo_update_successes += 1
                    else:
                        log_mongo_sample_fields("Failed to update", sample_doc)
                        counter_mongo_update_failures += 1

                except Exception as e:
                    counter_mongo_update_failures += 1
                    LOGGER.critical("Failed to update sample in Mongo for mongo id " f"{sample_doc[FIELD_MONGODB_ID]}")
                    LOGGER.exception(e)

    LOGGER.info(f"Count of successful Mongo updates = {counter_mongo_update_successes}")
    LOGGER.info(f"Count of failed Mongo updates = {counter_mongo_update_failures}")


def log_mongo_sample_fields(description, mongo_sample):
    DATA_LOGGER.info(f"Logging sample fields -- {description}")
    DATA_LOGGER.info(mongo_sample)


def update_mongo_sample(samples_collection: Collection, sample_doc: SampleDoc) -> bool:
    """Updates a sample in the Mongo samples collection

    Arguments:
        config {Config} -- application config specifying database details
        sample_doc {SampleDoc} -- the sample document whose fields should be updated

    Returns:
        bool -- whether the updates completed successfully
    """
    try:
        mongo_sample = samples_collection.find_one_and_update(
            filter={
                FIELD_MONGODB_ID: sample_doc[FIELD_MONGODB_ID],
            },
            update={
                "$set": {
                    FIELD_LH_SAMPLE_UUID: sample_doc[FIELD_LH_SAMPLE_UUID],
                    FIELD_UUID_UPDATED: sample_doc[FIELD_UUID_UPDATED],
                    FIELD_UPDATED_AT: sample_doc[FIELD_UPDATED_AT],
                }
            },
        )

        return mongo_sample is not None

    except Exception as e:
        LOGGER.critical("Failed to update sample in mongo for mongo id " f"{sample_doc[FIELD_MONGODB_ID]}")
        LOGGER.exception(e)

        return False


def check_samples_are_valid(
    config: Config,
    samples_collection: Collection,
    source_plate_barcodes: List[str],
) -> None:
    """
    Validate that samples do not have a sample uuid
    """
    samples_with_sample_uuid = list(
        samples_collection.find(
            {
                FIELD_PLATE_BARCODE: {"$in": source_plate_barcodes},
                "$and": [
                    {FIELD_LH_SAMPLE_UUID: {"$ne": None}},
                    {FIELD_LH_SAMPLE_UUID: {"$ne": ""}},
                ],
            }
        )
    )
    if len(samples_with_sample_uuid) > 0:
        raise ExceptionSampleWithSampleUUID(
            f"Some of the samples have a sample uuid: {extract_mongodb_ids(samples_with_sample_uuid)}"
        )

    """
    Validate that there are matching sample rows in both mongo and mlwh for the list of barcodes supplied
    i.e. that the sample rows are present in both databases ready to be updated
    """
    # select mongodb ids from mongo for barcodes list
    query_mongo = list(
        # fetch just the mongo ids
        samples_collection.find({FIELD_PLATE_BARCODE: {"$in": source_plate_barcodes}}, {FIELD_MONGODB_ID: 1})
    )

    list_mongo_ids = extract_mongodb_ids(query_mongo)

    if len(list_mongo_ids) == 0:
        raise ExceptionNoSamplesForGivenPlateBarcodes("There are no samples in Mongo for the given plate barcodes.")

    # select count of rows from MLWH for list_mongo_ids
    query = SQL_MLWH_COUNT_MONGO_IDS % {"mongo_ids": ",".join([f'"{mongo_id}"' for mongo_id in list_mongo_ids])}
    count_mlwh_rows = next(mysql_generator_from_config(config=config, query=query))["COUNT(*)"]

    # check numbers of rows matches
    count_mongo_rows = len(list_mongo_ids)
    if count_mongo_rows != count_mlwh_rows:
        raise ExceptionSampleCountsForMongoAndMLWHNotMatching(
            f"The number of samples for the list of barcodes in Mongo ({count_mongo_rows}) does not match "
            f"the number in MLWH ({count_mlwh_rows})."
        )
