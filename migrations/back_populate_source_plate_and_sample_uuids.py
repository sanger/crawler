import logging
import logging.config
import os
from datetime import datetime
from typing import Dict, List, Optional, cast
from uuid import uuid4

from mysql.connector.cursor_cext import CMySQLCursor
from pymongo.collection import Collection

from crawler.constants import (
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    FIELD_COORDINATE,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGO_LAB_ID,
    FIELD_MONGO_SOURCE_PLATE_BARCODE,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_UPDATED_AT,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.db.mysql import create_mysql_connection, run_mysql_executemany_query
from crawler.helpers.general_helpers import create_source_plate_doc, map_mongo_to_sql_common
from crawler.sql_queries import SQL_MLWH_COUNT_MONGO_IDS, SQL_MLWH_UPDATE_SAMPLE_UUID_PLATE_UUID
from crawler.types import Config, SampleDoc
from migrations.helpers.shared_helper import (
    extract_barcodes,
    extract_mongodb_ids,
    get_mongo_samples_for_source_plate,
    validate_args,
)

LOGGER = logging.getLogger(__name__)

SUPPRESS_ERROR_KEY_EXISTING_SAMPLE_UUIDS = "SUPPRESS_ERROR_FOR_EXISTING_SAMPLE_UUIDS"


class ExceptionSampleWithSampleUUIDNotSourceUUID(Exception):
    pass


class ExceptionSampleWithSourceUUIDNotSampleUUID(Exception):
    pass


class ExceptionSourcePlateDefined(Exception):
    pass


class ExceptionSampleCountsForMongoAndMLWHNotMatching(Exception):
    pass


"""
Assumptions:
1. checked source plates do not already have lh_source_plate_uuid or lh_sample_uuid in either
the mongo 'source_plate' or 'samples' collections, or in the MLWH lighthouse_sample table
  - Where Mongo does have lh_sample_uuid set but no lh_source_plate_uuid, an exception will be raised, but if you want
    to continue regardless, run the migration again with environment variable SUPPRESS_ERROR_FOR_EXISTING_SAMPLE_UUIDS
    set to true. Existing sample UUIDs will not be modified.
2. the samples do not have any duplicates for the same RNA Id in either mongo or MLWH

Csv file format: 'barcode' as the header on the first line, then one source plate barcode per line
e.g.
barcode
AP-12345678
AP-23456789
etc.

Steps:
1.  validate the file in the supplied filepath
2.  extract the source plate barcodes from the file
3.  iterate through the source plate barcodes
5.  select the samples in the source plate from mongo 'samples' collection, need mongo_id and lab_id
6.  iterate through the samples in the source plate:
7.      generate and insert a new source_plate row with a new lh_source_plate_uuid, using lab_id from first sample
8.      generate new lh_sample_uuid
9.      update sample in Mongo 'samples' to set lh_source_plate uuid, lh_sample_uuid, and updated_timestamp
10.     update sample in MLWH 'lighthouse_samples' to set lh_source_plate, lh_sample_uuid, and updated_timestamp
"""


def run(config: Config, s_filepath: str) -> None:
    filepath = validate_args(config=config, s_filepath=s_filepath)

    LOGGER.info("-" * 80)
    LOGGER.info("STARTING BACK POPULATING SOURCE PLATE AND SAMPLE UUIDS")
    LOGGER.info(f"Time start: {datetime.now()}")

    LOGGER.info(f"Starting update process with supplied file {filepath}")

    source_plate_barcodes = extract_barcodes(filepath=filepath)

    LOGGER.info(f"Source plate barcodes {source_plate_barcodes}")
    update_uuids_mongo_and_mlwh(config=config, source_plate_barcodes=source_plate_barcodes)


def check_samples_are_valid(
    config: Config,
    samples_collection: Collection,
    source_plates_collection: Collection,
    source_plate_barcodes: List[str],
) -> None:
    """
    Validate that none of the samples have a source plate uuid and raise error if that is
    the case
    """
    source_plates = list(
        source_plates_collection.find({FIELD_MONGO_SOURCE_PLATE_BARCODE: {"$in": source_plate_barcodes}})
    )
    if len(source_plates) > 0:
        raise ExceptionSourcePlateDefined(
            f"{len(source_plates)} plates are already present in the source plates because they may "
            f"have already been picked which is not supported by the script: {source_plates}"
        )

    """
    Validate that samples do not have a source plate uuid but no sample uuid
    """
    samples_only_source_plate = list(
        samples_collection.find(
            {
                FIELD_PLATE_BARCODE: {"$in": source_plate_barcodes},
                FIELD_LH_SOURCE_PLATE_UUID: {"$ne": None},
                FIELD_LH_SAMPLE_UUID: None,
            }
        )
    )
    if len(samples_only_source_plate) > 0:
        raise ExceptionSampleWithSourceUUIDNotSampleUUID(
            f"{len(samples_only_source_plate)}  of the samples have only a source plate uuid but no sample uuid. "
            f"Affected MongoDB IDs: {extract_mongodb_ids(samples_only_source_plate)}"
        )

    """
    Validate that samples do not have a sample uuid but no source plate uuid
    """
    samples_only_sample_uuid = list(
        samples_collection.find(
            {
                FIELD_PLATE_BARCODE: {"$in": source_plate_barcodes},
                FIELD_LH_SAMPLE_UUID: {"$ne": None},
                FIELD_LH_SOURCE_PLATE_UUID: None,
            }
        )
    )

    should_suppress_sample_uuid_error = (
        SUPPRESS_ERROR_KEY_EXISTING_SAMPLE_UUIDS in os.environ
        and os.environ[SUPPRESS_ERROR_KEY_EXISTING_SAMPLE_UUIDS].lower() != "false"
    )

    if len(samples_only_sample_uuid) > 0 and not should_suppress_sample_uuid_error:
        raise ExceptionSampleWithSampleUUIDNotSourceUUID(
            f"{len(samples_only_sample_uuid)} samples have a sample uuid but no source plate uuid. "
            f"Suppress this exception by setting the '{SUPPRESS_ERROR_KEY_EXISTING_SAMPLE_UUIDS}' "
            f"environment variable to true.\n\n Affected MongoDB IDs: {extract_mongodb_ids(samples_only_sample_uuid)}"
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

    list_mongo_ids = [str(x[FIELD_MONGODB_ID]) for x in query_mongo]

    # select count of rows from MLWH for list_mongo_ids
    count_mlwh_rows = mlwh_count_samples_from_mongo_ids(config, list_mongo_ids)

    # check numbers of rows matches
    count_mongo_rows = len(list_mongo_ids)
    if count_mongo_rows != count_mlwh_rows:
        raise ExceptionSampleCountsForMongoAndMLWHNotMatching(
            f"The number of samples for the list of barcodes in Mongo does not match"
            f"the number in the MLWH: {count_mongo_rows}!={count_mlwh_rows}"
        )


def update_uuids_mongo_and_mlwh(config: Config, source_plate_barcodes: List[str]) -> None:
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
    counter_mlwh_update_successes = 0
    counter_mlwh_update_failures = 0

    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)
        source_plates_collection = get_mongo_collection(mongo_db, COLLECTION_SOURCE_PLATES)

        check_samples_are_valid(config, samples_collection, source_plates_collection, source_plate_barcodes)

        for source_plate_barcode in source_plate_barcodes:
            LOGGER.info(f"Processing source plate barcode {source_plate_barcode}")

            # List[SampleDoc]
            sample_docs = get_mongo_samples_for_source_plate(samples_collection, source_plate_barcode)

            # iterate through samples
            current_source_plate_uuid = None
            for sample_doc in sample_docs:
                # will every sample doc have a plate_barcode and lab id?
                LOGGER.info(f"Sample in well {sample_doc[FIELD_COORDINATE]}")

                if current_source_plate_uuid is None:
                    # extract lab id from sample doc
                    lab_id = cast(str, sample_doc[FIELD_MONGO_LAB_ID])
                    LOGGER.info(f"Creating a source_plate collection row with lab id = {lab_id}")
                    # create source_plate record and extract lh_source_plate_uuid for next samples
                    current_source_plate_uuid = create_mongo_source_plate_record(
                        source_plates_collection, source_plate_barcode, lab_id
                    )

                sample_doc[FIELD_LH_SOURCE_PLATE_UUID] = current_source_plate_uuid
                # generate an lh_sample_uuid if the sample doesn't have one
                if FIELD_LH_SAMPLE_UUID not in sample_doc or (sample_doc[FIELD_LH_SAMPLE_UUID] is None):
                    sample_doc[FIELD_LH_SAMPLE_UUID] = str(uuid4())

                # update sample in Mongo ‘samples’ to set lh_source_plate uuid, lh_sample_uuid, and updated_timestamp
                try:
                    sample_doc[FIELD_UPDATED_AT] = datetime.utcnow()
                    success = update_mongo_sample_uuid_and_source_plate_uuid(samples_collection, sample_doc)
                    if success:
                        counter_mongo_update_successes += 1
                    else:
                        counter_mongo_update_failures += 1

                except Exception as e:
                    counter_mongo_update_failures += 1
                    LOGGER.critical("Failed to update sample in Mongo for mongo id " f"{sample_doc[FIELD_MONGODB_ID]}")
                    LOGGER.exception(e)

                # update sample in MLWH 'lighthouse_samples' to set lh_source_plate,
                # lh_sample_uuid, and updated_timestamp
                sample_doc[FIELD_MONGODB_ID] = str(sample_doc[FIELD_MONGODB_ID])
                try:
                    success = update_mlwh_sample_uuid_and_source_plate_uuid(config, sample_doc)
                    if success:
                        counter_mlwh_update_successes += 1
                    else:
                        counter_mlwh_update_failures += 1
                except Exception as e:
                    counter_mlwh_update_failures += 1
                    LOGGER.critical("Failed to update sample in MLWH for mongo id " f"{sample_doc[FIELD_MONGODB_ID]}")
                    LOGGER.exception(e)

    LOGGER.info(f"Count of successful Mongo updates = {counter_mongo_update_successes}")
    LOGGER.info(f"Count of failed Mongo updates = {counter_mongo_update_failures}")
    LOGGER.info(f"Count of successful MLWH updates = {counter_mlwh_update_successes}")
    LOGGER.info(f"Count of failed MLWH updates = {counter_mlwh_update_failures}")

    return


def update_mongo_sample_uuid_and_source_plate_uuid(samples_collection: Collection, sample_doc: SampleDoc) -> bool:
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
                    FIELD_LH_SOURCE_PLATE_UUID: sample_doc[FIELD_LH_SOURCE_PLATE_UUID],
                    FIELD_UPDATED_AT: sample_doc[FIELD_UPDATED_AT],
                }
            },
        )
        if mongo_sample is None:
            return False
        else:
            return True

    except Exception as e:
        LOGGER.critical("Failed to update sample in mongo for mongo id " f"{sample_doc[FIELD_MONGODB_ID]}")
        LOGGER.exception(e)
    return False


def update_mlwh_sample_uuid_and_source_plate_uuid(config: Config, sample_doc: SampleDoc) -> bool:
    """Updates a sample in the sample in the MLWH database

    Arguments:
        config {Config} -- application config specifying database details
        sample_doc {SampleDoc} -- the sample document whose fields should be updated

    Returns:
        bool -- whether the updates completed successfully
    """
    mysql_conn = create_mysql_connection(config, False)

    if mysql_conn is not None and mysql_conn.is_connected():
        sample_mongo = map_mongo_to_sql_common(sample_doc)
        sample_mongo[FIELD_UPDATED_AT] = datetime.now()
        run_mysql_executemany_query(
            mysql_conn, SQL_MLWH_UPDATE_SAMPLE_UUID_PLATE_UUID, [cast(Dict[str, str], sample_mongo)]
        )
        return True
    else:
        return False


def mlwh_count_samples_from_mongo_ids(config: Config, mongo_ids: List[str]) -> int:
    """Count samples from mongo_ids

    Arguments:
        config {Config} -- application config specifying database details
        mongo_ids {List[str]} -- the list of mongo_ids to find

    Returns:
        int -- number of samples
    """
    mysql_conn = create_mysql_connection(config, False)

    if mysql_conn is not None and mysql_conn.is_connected():
        cursor: CMySQLCursor = mysql_conn.cursor()
        query_str = SQL_MLWH_COUNT_MONGO_IDS % {"mongo_ids": str(mongo_ids).strip("[]")}
        cursor.execute(query_str)
        return cast(int, cursor.fetchone()[0])
    else:
        raise Exception("Cannot connect mysql")


def create_mongo_source_plate_record(
    source_plates_collection: Collection, source_plate_barcode: str, lab_id: str
) -> Optional[str]:
    """Creates a mongo source_plate collection row

    Arguments:
        mongo_db {Database} -- the mongo database connection
        source_plate_barcode {str} -- the barcode of the source plate
        lab_id {str} -- the lab id for the sample

    Returns:
        bool -- whether the updates completed successfully
    """
    try:
        new_plate_doc = create_source_plate_doc(source_plate_barcode, lab_id)
        new_plate_uuid = new_plate_doc[FIELD_LH_SOURCE_PLATE_UUID]

        LOGGER.debug(f"Attempting to insert new source plate for barcode {source_plate_barcode} and lab id {lab_id}")
        source_plates_collection.insert_one(new_plate_doc)

        return cast(str, new_plate_uuid)

    except Exception as e:
        LOGGER.critical(f"Error inserting a source plate row for barcode {source_plate_barcode} and lab id {lab_id}")
        LOGGER.exception(e)

    return None
