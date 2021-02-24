#
# This helper module will contain all functions required for running Step 2
#
import logging
import logging.config

from typing import Any, List, Dict, Final
from crawler.types import ModifiedRow, Config, SampleDoc, SampleDocValue
from crawler.db.mongo import (
    get_mongo_collection,
)
from pymongo.database import Database

from crawler.constants import (
    COLLECTION_PRIORITY_SAMPLES,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_MUST_SEQUENCE,
    FIELD_PROCESSED,
    FIELD_SAMPLE_ID,
    FIELD_MONGODB_ID,
    FIELD_PREFERENTIALLY_SEQUENCE,
    FIELD_PLATE_BARCODE,
    DART_STATE_PENDING,
    FIELD_SOURCE,
)

from crawler.helpers.general_helpers import (
    map_mongo_sample_to_mysql,
)

from more_itertools import groupby_transform

from crawler.helpers.logging_helpers import LoggingCollection

from crawler.db.mysql import create_mysql_connection, run_mysql_executemany_query

from crawler.db.dart import (
    create_dart_sql_server_conn,
    add_dart_plate_if_doesnt_exist,
    add_dart_well_properties,
)

from crawler.sql_queries import SQL_MLWH_MULTIPLE_INSERT

logger = logging.getLogger(__name__)

logging_collection = LoggingCollection()


# Possibly move to seperate script
def step_two(db: Database, config: Config) -> None:
    """
    Description
    Arguments:
        x {Type} -- description
    """
    def extract_mongo_id(sample: SampleDoc) -> SampleDocValue:
        return sample[FIELD_MONGODB_ID]

    logger.info("Starting Step 2")

    samples = query_any_unprocessed_samples(db)

    # Create all samples in MLWH with samples containing both sample and priority sample info
    mlwh_success = update_priority_samples_into_mlwh(samples, config)

    # Add to the DART database if MLWH update was successful
    if mlwh_success:
        logger.info("MLWH insert successful and adding to DART")

        dart_success = insert_plates_and_wells_into_dart(samples, config)
        if dart_success:
            # use stored identifiers to update priority_samples table to processed true
            sample_ids = list(map(extract_mongo_id, samples))
            update_unprocessed_priority_samples_to_processed(db, sample_ids)


def query_any_unprocessed_samples(db: Database) -> List[Any]:
    priority_samples_collection = get_mongo_collection(db, COLLECTION_PRIORITY_SAMPLES)

    # Query:
    # from the priority samples collection
    # get all unprocessed samples, we want to update also samples that have changed their importance
    # join on the samples collection using sample_id in priority_samples collection to _id in the samples collection
    # flatten object so sample fields are at the same level as the priority samples fields, removing
    # the nested sample object return a list of the samples
    # e.g db.priority_samples.aggregate([{"$match":{"$and": [
    # {"processed": false} ]}}, {"$lookup": {"from": "samples", "let": {"sample_id": "$_id"},"pipeline":
    # [{"$match": {"$expr": {"$and":[{"$eq": ["$sample_id", "$$sample_id"]}]}}}], "as": "from_samples"}}])
    IMPORTANT_UNPROCESSED_SAMPLES_MONGO_QUERY: Final[List[object]] = [
        # first get only unprocessed priority samples
        {
            "$match": {
                "$and": [
                    {FIELD_PROCESSED: False},
                ]
            }
        },
        {
            "$lookup": {
                "from": "samples",
                "localField": "sample_id",
                "foreignField": FIELD_MONGODB_ID,
                "as": "related_samples",
            }
        },
        # replace the document with a merge of the original and the first element of the array created from the lookup
        # above - this should always be 1 element
        {"$replaceRoot": {"newRoot": {"$mergeObjects": [{"$arrayElemAt": ["$related_samples", 0]}, "$$ROOT"]}}},
        # result = { mongo_sample_id=1234, must_sequence=true; related_samples: { uuid: 132 } }
        # remove the lookup document
        {"$project": {"related_samples": 0}}
        # result = { mongo_sample_id=1234, must_sequence=true, uuid: 132 }
    ]

    value = priority_samples_collection.aggregate(IMPORTANT_UNPROCESSED_SAMPLES_MONGO_QUERY)
    return list(value)


def merge_priority_samples_into_docs_to_insert(priority_samples: List[Any], docs_to_insert: list) -> None:
    """
    Updates the sample records with must_sequence and preferentially_sequence values

    for each successful add sample, merge into docs_to_insert_mlwh
    with must_sequence and preferentially_sequence values

    Arguments:
        priority_samples  - priority samples to update docs_to_insert with
        docs_to_insert {List[ModifiedRow]} -- the sample records to update
    """

    def extract_root_sample_id(sample: SampleDoc) -> SampleDocValue:
        return sample[FIELD_ROOT_SAMPLE_ID]

    priority_root_sample_ids = list(map(extract_root_sample_id, priority_samples))

    for doc in docs_to_insert:
        root_sample_id = doc[FIELD_ROOT_SAMPLE_ID]
        if root_sample_id in priority_root_sample_ids:
            priority_sample = list(filter(lambda x: x[FIELD_ROOT_SAMPLE_ID] == root_sample_id, priority_samples))[0]
            doc[FIELD_MUST_SEQUENCE] = priority_sample[FIELD_MUST_SEQUENCE]
            doc[FIELD_PREFERENTIALLY_SEQUENCE] = priority_sample[FIELD_PREFERENTIALLY_SEQUENCE]


# TODO: refactor duplicated function
def update_unprocessed_priority_samples_to_processed(db: Database, sample_ids: list) -> None:
    """
    Description
    use stored identifiers to update priority_samples table to processed true
    Arguments:
        x {Type} -- description
    """
    priority_samples_collection = get_mongo_collection(db, COLLECTION_PRIORITY_SAMPLES)
    for sample_id in sample_ids:
        priority_samples_collection.update({FIELD_SAMPLE_ID: sample_id}, {"$set": {FIELD_PROCESSED: True}})
    logger.info("Mongo update of processed for priority samples successful")


# TODO: refactor duplicated function
def update_priority_samples_into_mlwh(samples: List[Any], config: Config) -> bool:
    """Insert sample records into the MLWH database from the parsed file information, including the corresponding
    mongodb _id
    Create all samples in MLWH with samples including must_seq/ pre_seq

    Arguments:
        samples {List[ModifiedRow]} -- List of filtered sample information extracted from CSV files.

    Returns:
        {bool} -- True if the insert was successful; otherwise False
    """

    values: List[Dict[str, Any]] = []

    for sample_doc in samples:
        values.append(map_mongo_sample_to_mysql(sample_doc))

    mysql_conn = create_mysql_connection(config, False)

    if mysql_conn is not None and mysql_conn.is_connected():
        try:
            run_mysql_executemany_query(mysql_conn, SQL_MLWH_MULTIPLE_INSERT, values)

            logger.debug("MLWH database inserts completed successfully for priority samples")
            return True
        except Exception as e:
            logging_collection.add_error(
                "TYPE 28",
                "MLWH database inserts failed for priority samples",
            )
            logger.critical(f"Critical error while processing priority samples': {e}")
            logger.exception(e)
    else:
        logging_collection.add_error(
            "TYPE 29",
            "MLWH database inserts failed, could not connect",
        )
        logger.critical("Error writing to MLWH for priority samples, could not create Database connection")

    return False


# TODO: refactor duplicated function
def insert_plates_and_wells_into_dart(docs_to_insert: List[ModifiedRow], config: Config) -> bool:
    """Insert plates and wells into the DART database.
    Create in DART with docs_to_insert including must_seq/ pre_seq
    use docs_to_insert to update DART

    Arguments:
        docs_to_insert {List[ModifiedRow]} -- List of filtered sample information extracted from CSV files.

    Returns:
        TODO: check return False
        {bool} -- True if the insert was successful; otherwise False
    """
    if (sql_server_connection := create_dart_sql_server_conn(config)) is not None:
        try:
            cursor = sql_server_connection.cursor()
            # check docs_to_insert contain must_seq/ pre_seq
            for plate_barcode, samples in groupby_transform(  # type: ignore
                docs_to_insert, lambda x: x[FIELD_PLATE_BARCODE]
            ):
                try:
                    samples = list(samples)
                    centre_config = centre_config_for_samples(config, samples)
                    plate_state = add_dart_plate_if_doesnt_exist(
                        cursor, plate_barcode, centre_config["biomek_labware_class"]  # type: ignore
                    )
                    if plate_state == DART_STATE_PENDING:
                        for sample in samples:
                            add_dart_well_properties(cursor, sample, plate_barcode)  # type: ignore
                    cursor.commit()
                except Exception as e:
                    logging_collection.add_error(
                        "TYPE 33",
                        f"DART database inserts failed for plate {plate_barcode} in priority samples inserts",
                    )
                    logger.exception(e)
                    # rollback statements executed since previous commit/rollback
                    cursor.rollback()
                    return False

            logger.debug("DART database inserts completed successfully for priority samples")
            return True
        except Exception as e:
            logging_collection.add_error(
                "TYPE 30",
                "DART database inserts failed for priority samples",
            )
            logger.critical(f"Critical error for priority samples: {e}")
            logger.exception(e)
            return False
        finally:
            sql_server_connection.close()
    else:
        logging_collection.add_error(
            "TYPE 31",
            "DART database inserts failed, could not connect, for priority samples",
        )
        logger.critical("Error writing to DART for priority samples, could not create Database connection")
        return False


def centre_config_for_samples(config, samples):
    centre_name = samples[0][FIELD_SOURCE]

    return list(filter(lambda x: x["name"] == centre_name, config.CENTRES))[0]
