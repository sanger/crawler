import logging
import logging.config
from datetime import datetime
from typing import Any, Dict, Iterator, List, Tuple

from bson.objectid import ObjectId
from pymongo.collection import Collection

from crawler.constants import (
    COLLECTION_SAMPLES,
    FIELD_CREATED_AT,
    FIELD_LH_SAMPLE_UUID,
    FIELD_MONGODB_ID,
    FIELD_UPDATED_AT,
    MLWH_LH_SAMPLE_UUID,
    MLWH_MONGODB_ID,
    MONGO_DATETIME_FORMAT,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.db.mysql import create_mysql_connection
from crawler.helpers.cherrypicked_samples import extract_required_cp_info
from crawler.types import Config, SampleDoc
from migrations.helpers.shared_helper import valid_datetime_string
from migrations.helpers.update_filtered_positives_helper import update_dart_fields

logger = logging.getLogger(__name__)

UUID_UPDATED = "uuid_updated"

"""
1. get list of all samples from lighthouse_sample table where updated_at is a known timestamp
2. iterate through 1. by matching the `_id` with `mongodb_id`; if the `sample_uuid` is different, overwrite and add a
flag that the UUID has been updated
3. get all the mongo samples that have had their UUID updated
4. run the DART migration over these samples for the same date which was used when populating DART
"""


def run(config: Config, s_start_datetime: str, s_end_datetime: str, updated_at: str) -> None:
    start_datetime, end_datetime, updated_at_datetime = validate_args(
        config=config, s_start_datetime=s_start_datetime, s_end_datetime=s_end_datetime, updated_at=updated_at
    )

    logger.info("-" * 80)
    logger.info("STARTING BACK POPULATING UUIDS")
    logger.info(f"Time start: {datetime.now()}")

    logger.info(f"Starting update process with Start datetime {start_datetime} and End datetime {end_datetime}")

    update_mongo(config=config, updated_at=updated_at_datetime)

    update_dart(config=config, start_datetime=start_datetime, end_datetime=end_datetime)


def validate_args(
    config: Config, s_start_datetime: str, s_end_datetime: str, updated_at: str
) -> Tuple[datetime, datetime, datetime]:
    base_msg = "Aborting run: "
    if not config:
        msg = f"{base_msg} Config required"
        logger.error(msg)
        raise Exception(msg)

    if not valid_datetime_string(s_start_datetime):
        msg = f"{base_msg} Expected format of Start datetime is YYMMDD_HHmm"
        logger.error(msg)
        raise Exception(msg)

    if not valid_datetime_string(s_end_datetime):
        msg = f"{base_msg} Expected format of End datetime is YYMMDD_HHmm"
        logger.error(msg)
        raise Exception(msg)

    if not valid_datetime_string(updated_at):
        msg = f"{base_msg} Expected format of updated_at datetime is YYMMDD_HHmm"
        logger.error(msg)
        raise Exception(msg)

    start_datetime = datetime.strptime(s_start_datetime, MONGO_DATETIME_FORMAT)
    end_datetime = datetime.strptime(s_end_datetime, MONGO_DATETIME_FORMAT)
    updated_at_datetime = datetime.strptime(updated_at, MONGO_DATETIME_FORMAT)

    if start_datetime > end_datetime:
        msg = f"{base_msg} End datetime must be greater than Start datetime"
        logger.error(msg)
        raise Exception(msg)

    return start_datetime, end_datetime, updated_at_datetime


def update_mongo(config: Config, updated_at: datetime) -> None:
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)

        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        counter = 0
        for mysql_sample in mysql_sample_generator(
            config=config,
            query=f"SELECT * FROM lighthouse_sample WHERE updated_at > '{updated_at.strftime('%Y-%m-%d %H:%M')}'",
        ):
            mlwh_sample_uuid = mysql_sample.get(MLWH_LH_SAMPLE_UUID)

            if mlwh_sample_uuid is None:
                continue

            mongo_sample = samples_collection.find_one_and_update(
                filter={
                    FIELD_MONGODB_ID: ObjectId(mysql_sample.get(MLWH_MONGODB_ID)),
                    FIELD_LH_SAMPLE_UUID: {
                        "$ne": mlwh_sample_uuid,
                    },
                },
                update={
                    "$set": {
                        FIELD_LH_SAMPLE_UUID: mlwh_sample_uuid,
                        UUID_UPDATED: True,
                        FIELD_UPDATED_AT: datetime.utcnow(),
                    }
                },
            )

            if mongo_sample is not None:
                counter += 1

            if counter > 0 and (counter % 5000) == 0:
                logger.debug(f"{counter = }")

        logger.debug(f"{counter} samples updated in mongo")


def mysql_sample_generator(config: Config, query: str) -> Iterator[Dict[str, Any]]:
    with create_mysql_connection(config=config, readonly=True) as connection:
        with connection.cursor(dictionary=True, buffered=False) as cursor:
            cursor.execute(query)
            for row in cursor:
                yield row


def update_dart(config: Config, start_datetime: datetime, end_datetime: datetime) -> None:
    try:
        with create_mongo_client(config) as client:
            mongo_db = get_mongo_db(config, client)

            samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

            # get samples from mongo between these time ranges and with updated UUIDs
            samples = get_samples(samples_collection, start_datetime, end_datetime)

        if not samples:
            logger.info("No samples in this time range and with updated UUIDs")
            return

        logger.debug(f"{len(samples)} samples to process")

        _, plate_barcodes = extract_required_cp_info(samples)

        logger.debug(f"{len(plate_barcodes)} unique plate barcodes")

        update_dart_fields(config, samples)
    except Exception as e:
        logger.error("Error while attempting to migrate all DBs")
        logger.exception(e)


def get_samples(samples_collection: Collection, start_datetime: datetime, end_datetime: datetime) -> List[SampleDoc]:
    logger.debug(f"Selecting samples between {start_datetime} and {end_datetime}")

    match = {
        "$match": {
            # Filter by the start and end dates and UUID updated
            FIELD_CREATED_AT: {"$gte": start_datetime, "$lte": end_datetime},
            UUID_UPDATED: True,
        }
    }

    return list(samples_collection.aggregate([match]))
