import logging
import logging.config
import os
from contextlib import closing
from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Optional, cast
from uuid import uuid4

from mysql.connector.connection_cext import MySQLConnectionAbstract
from pymongo.collection import Collection
from pymongo.database import Database

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
    MLWH_MONGODB_ID,
    MLWH_UPDATED_AT,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.db.mysql import create_mysql_connection, run_mysql_executemany_query
from crawler.helpers.general_helpers import create_source_plate_doc, map_mongo_to_sql_common
from crawler.sql_queries import (
    SQL_MLWH_COUNT_MONGO_IDS,
    SQL_MLWH_GET_SAMPLES_FOR_MONGO_IDS,
    SQL_MLWH_UPDATE_SAMPLE_UUID_PLATE_UUID,
)
from crawler.types import Config, SampleDoc
from migrations.helpers.shared_helper import (
    extract_barcodes,
    extract_mongodb_ids,
    get_mongo_samples_for_source_plate,
    mysql_generator_from_connection,
    validate_args,
)

LOGGER = logging.getLogger(__name__)
DATA_LOGGER = logging.getLogger("migration_data")

SUPPRESS_ERROR_KEY_EXISTING_SAMPLE_UUIDS = "SUPPRESS_ERROR_FOR_EXISTING_SAMPLE_UUIDS"

RECORD_KEY_ORIGINAL = "original"
RECORD_KEY_UPDATED = "updated"


class ExceptionSampleWithSampleUUIDNotSourceUUID(Exception):
    pass


class ExceptionSampleWithSourceUUIDNotSampleUUID(Exception):
    pass


class ExceptionSourcePlateDefined(Exception):
    pass


class ExceptionSampleCountsForMongoAndMLWHNotMatching(Exception):
    pass


class MongoUpdateError(Exception):
    pass


class MLWHUpdateError(Exception):
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
    mysql_conn: MySQLConnectionAbstract,
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
    count_mlwh_rows = mlwh_count_samples_from_mongo_ids(mysql_conn, list_mongo_ids)

    # check numbers of rows matches
    count_mongo_rows = len(list_mongo_ids)
    if count_mongo_rows != count_mlwh_rows:
        raise ExceptionSampleCountsForMongoAndMLWHNotMatching(
            f"The number of samples for the list of barcodes in Mongo does not match "
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
    counter_mlwh_update_successes = 0

    with closing(create_mysql_connection(config, False)) as mysql_conn:
        with create_mongo_client(config) as client:
            mongo_db = get_mongo_db(config, client)
            samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)
            source_plates_collection = get_mongo_collection(mongo_db, COLLECTION_SOURCE_PLATES)

            check_samples_are_valid(mysql_conn, samples_collection, source_plates_collection, source_plate_barcodes)

            for source_plate_barcode in source_plate_barcodes:
                LOGGER.info(f"Processing source plate barcode {source_plate_barcode}")

                # List[SampleDoc]
                sample_docs = get_mongo_samples_for_source_plate(samples_collection, source_plate_barcode)

                if len(sample_docs) == 0:
                    continue

                # extract lab id from sample doc
                lab_id = cast(str, sample_docs[0][FIELD_MONGO_LAB_ID])
                LOGGER.info(f"Creating a source_plate collection row with lab id = {lab_id}")

                # create source_plate record and extract lh_source_plate_uuid for updating samples
                current_source_plate_uuid = create_mongo_source_plate_record(
                    source_plates_collection, source_plate_barcode, lab_id
                )

                records = [{RECORD_KEY_ORIGINAL: doc} for doc in sample_docs]
                for record in records:
                    original_doc = record[RECORD_KEY_ORIGINAL]
                    LOGGER.info(f"Processing sample in well {original_doc[FIELD_COORDINATE]}")

                    # Update the record
                    updated_doc = deepcopy(original_doc)

                    updated_doc[FIELD_UPDATED_AT] = datetime.utcnow()
                    updated_doc[FIELD_LH_SOURCE_PLATE_UUID] = current_source_plate_uuid
                    # Generate an lh_sample_uuid if the sample doesn't have one
                    if FIELD_LH_SAMPLE_UUID not in updated_doc or (updated_doc[FIELD_LH_SAMPLE_UUID] is None):
                        updated_doc[FIELD_LH_SAMPLE_UUID] = str(uuid4())

                    record[RECORD_KEY_UPDATED] = updated_doc

                # Do MongoDB updates
                try:
                    update_mongo_sample_uuids(mongo_db, records)
                    counter_mongo_update_successes += len(records)
                except Exception as e:
                    LOGGER.critical(f"Failed to update MongoDB for plate with barcode '{source_plate_barcode}'.")
                    LOGGER.exception(e)
                    raise

                # Do MLWH updates
                try:
                    update_mlwh_sample_uuids(mysql_conn, [r[RECORD_KEY_UPDATED] for r in records])
                    counter_mlwh_update_successes += len(records)
                except Exception as e:
                    LOGGER.critical(f"Failed to update MLWH for plate with barcode '{source_plate_barcode}'.")
                    LOGGER.exception(e)
                    raise

    LOGGER.info(f"Count of successful Mongo updates = {counter_mongo_update_successes}")
    LOGGER.info(f"Count of successful MLWH updates = {counter_mlwh_update_successes}")


def log_mongo_sample_fields(description, mongo_samples):
    DATA_LOGGER.info(f"=== Logging Mongo sample fields -- {description} ===")
    for sample in mongo_samples:
        DATA_LOGGER.info(sample)
    DATA_LOGGER.info(f"=== End of Mongo sample fields -- {description} ===")


def update_mongo_sample_uuids(mongo_db: Database, records: list) -> None:
    """Updates a list of samples in the Mongo samples collection.

    Arguments:
        mongo_db {Database} -- the Mongo database to update.
        records {List} -- a list of sample documents to find and update.
    """
    log_mongo_sample_fields("Before update", [r[RECORD_KEY_ORIGINAL] for r in records])

    with mongo_db.client.start_session() as session:
        with session.start_transaction():
            samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

            for record in records:
                update_doc = record[RECORD_KEY_UPDATED]

                try:
                    mongo_sample = samples_collection.find_one_and_update(
                        filter={
                            FIELD_MONGODB_ID: update_doc[FIELD_MONGODB_ID],
                        },
                        update={
                            "$set": {
                                FIELD_LH_SAMPLE_UUID: update_doc[FIELD_LH_SAMPLE_UUID],
                                FIELD_LH_SOURCE_PLATE_UUID: update_doc[FIELD_LH_SOURCE_PLATE_UUID],
                                FIELD_UPDATED_AT: update_doc[FIELD_UPDATED_AT],
                            }
                        },
                    )

                    if mongo_sample is None:
                        raise MongoUpdateError("No document was returned for find_one_and_update.")

                except Exception as e:
                    LOGGER.critical("Failed to update sample in mongo for mongo id " f"{update_doc[FIELD_MONGODB_ID]}")
                    LOGGER.exception(e)
                    session.abort_transaction()
                    raise

            session.commit_transaction()

    log_mongo_sample_fields("After successful update", [r[RECORD_KEY_UPDATED] for r in records])


def log_mlwh_sample_fields(description, mlwh_samples):
    DATA_LOGGER.info(f"=== Logging MLWH sample fields -- {description} ===")
    for sample in mlwh_samples:
        DATA_LOGGER.info(sample)
    DATA_LOGGER.info(f"=== End of MLWH sample fields -- {description} ===")


def update_mlwh_sample_uuids(mysql_conn: MySQLConnectionAbstract, sample_docs: List[SampleDoc]) -> None:
    """Updates a list of samples in the MLWH database.

    Arguments:
        mysql_conn {MySQLConnectionAbstract} -- a connection to the MLWH MySQL database.
        sample_docs {List[SampleDoc]} -- the sample documents whose fields should be updated.
    """

    # Convert MongoDB docs into MLWH rows
    def prepare_mlwh_row(sample_doc):
        mlwh_row = map_mongo_to_sql_common(sample_doc)
        mlwh_row[MLWH_UPDATED_AT] = datetime.now()
        return mlwh_row

    update_rows = [prepare_mlwh_row(s) for s in sample_docs]
    mongo_ids = [doc[MLWH_MONGODB_ID] for doc in update_rows]

    if mysql_conn is None or not mysql_conn.is_connected():
        raise MLWHUpdateError("The MySQL connection is not ready to perform updates.")

    # Log the current fields on the MLWH samples
    query = SQL_MLWH_GET_SAMPLES_FOR_MONGO_IDS % {"mongo_ids": str(mongo_ids).strip("[]")}
    existing_samples = mysql_generator_from_connection(mysql_conn, query)
    log_mlwh_sample_fields("Before update", existing_samples)

    row_data = [cast(Dict[str, str], row) for row in update_rows]
    run_mysql_executemany_query(mysql_conn, SQL_MLWH_UPDATE_SAMPLE_UUID_PLATE_UUID, row_data)

    # Log the new fields on the MLWH samples
    post_update_samples = mysql_generator_from_connection(mysql_conn, query)
    log_mlwh_sample_fields("After update", post_update_samples)


def mlwh_count_samples_from_mongo_ids(mysql_conn: MySQLConnectionAbstract, mongo_ids: List[str]) -> int:
    """Count samples from mongo_ids

    Arguments:
        mysql_conn {MySQLConnectionAbstract} -- a connection to the MLWH MySQL database
        mongo_ids {List[str]} -- the list of mongo_ids to find

    Returns:
        int -- number of samples
    """
    if mysql_conn is not None and mysql_conn.is_connected():
        query_str = SQL_MLWH_COUNT_MONGO_IDS % {"mongo_ids": str(mongo_ids).strip("[]")}

        with closing(mysql_conn.cursor()) as cursor:
            cursor.execute(query_str)
            result = cursor.fetchone()

        if result is None:
            raise Exception("Query result was not valid")

        return cast(int, result[0])
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

        DATA_LOGGER.info(f"Inserted new source plate Mongo document: {new_plate_doc}")

        return cast(str, new_plate_uuid)

    except Exception as e:
        LOGGER.critical(f"Error inserting a source plate row for barcode {source_plate_barcode} and lab id {lab_id}")
        LOGGER.exception(e)

    return None
