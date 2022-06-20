import logging
import logging.config
import os
import stat
from csv import DictReader
from datetime import datetime
from typing import List
from uuid import uuid

from pymongo.collection import Collection
from pymongo.database import Database

from crawler.constants import (COLLECTION_SAMPLES, COLLECTION_SOURCE_PLATES,
                               FIELD_LH_SAMPLE_UUID,
                               FIELD_LH_SOURCE_PLATE_UUID, FIELD_MONGO_LAB_ID,
                               FIELD_MONGODB_ID, FIELD_PLATE_BARCODE,
                               FIELD_UPDATED_AT)
from crawler.db.mongo import (create_mongo_client, get_mongo_collection,
                              get_mongo_db)
from crawler.db.mysql import (create_mysql_connection, run_mysql_executemany_query)
from crawler.helpers.general_helpers import (create_source_plate_doc)
from crawler.sql_queries import SQL_MLWH_UPDATE_SAMPLE_UUID_PLATE_UUID
from crawler.types import Config, SampleDoc


logger = logging.getLogger(__name__)

"""
Assumptions:
1. checked source plates do not already have lh_source_plate_uuid or lh_sample_uuid in either
the mongo 'source_plate' or 'samples' collections, or in the MLWH lighthouse_sample table
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
9.      update sample in Mongo ‘samples’ to set lh_source_plate uuid, lh_sample_uuid, and updated_timestamp
10.     update sample in MLWH 'lighthouse_samples' to set lh_source_plate, lh_sample_uuid, and updated_timestamp
"""

def run(config: Config, s_filepath: str) -> None:
    filepath = validate_args(
        config=config, s_filepath=s_filepath
    )

    logger.info("-" * 80)
    logger.info("STARTING BACK POPULATING SOURCE PLATE AND SAMPLE UUIDS")
    logger.info(f"Time start: {datetime.now()}")

    logger.info(f"Starting update process with supplied file {filepath}")

    source_plate_barcodes = extract_barcodes(
      config=config, filepath=filepath
    )

    logger.info(f"Source plate barcodes {source_plate_barcodes}")
    update_uuids_mongo_and_mlwh(config=config, source_plate_barcodes=source_plate_barcodes)


def validate_args(config: Config, s_filepath: str) -> str:
    """Validate the supplied arguments

    Arguments:
        config {Config} -- application config specifying database details
        s_filepath {str} -- the filepath of the csv file containing the list of source plate barcodes

    Returns:
        str -- the filepath if valid
    """
    base_msg = "Aborting run: "
    if not config:
        msg = f"{base_msg} Config required"
        logger.error(msg)
        raise Exception(msg)

    if not valid_filepath(s_filepath):
        msg = f"{base_msg} Unable to confirm valid csv file from supplied filepath"
        logger.error(msg)
        raise Exception(msg)

    filepath =s_filepath

    return filepath


def valid_filepath(s_filepath: str) -> bool:
    """Determine if the filepath argument supplied corresponds to a csv file

    Arguments:
        s_filepath {str} -- the filepath of the csv file containing the list of source plate barcodes

    Returns:
        bool -- whether the filepath corresponds to a csv file
    """
    if stat.S_ISREG(os.lstat(s_filepath).st_mode):
        file_name, file_extension = os.path.splitext(file_name)
        return file_extension == ".csv"

    return False


def extract_barcodes(config: Config, filepath: str) -> List[str]:
    """Extract the list of barcodes from the csv file

    Arguments:
        config {Config} -- application config specifying database details
        filepath {str} -- the filepath of the csv file containing the list of source plate barcodes

    Returns:
        List[str] -- list of source plate barcodes
    """
    extracted_barcodes : List[str] = []
    try:
        with open(filepath, newline="") as csvfile:
          csvreader = DictReader(csvfile)
          for row in csvreader:
            extracted_barcodes.append(row['barcode'])

    except Exception as e:
          logger.critical("Error reading source barcodes file " f"{filepath}")
          logger.exception(e)

    return extracted_barcodes


def update_uuids_mongo_and_mlwh(config: Config, source_plate_barcodes: List[str]):
    """Updates source plate and sample uuids in both mongo and mlwh

    Arguments:
        config {Config} -- application config specifying database details
        source_plate_barcodes {List[str]} -- the list of source plate barcodes

    Returns:
        Nothing
    """
    # counters to track progress
    counter_mongo_update_successes= 0
    counter_mongo_update_failures= 0
    counter_mlwh_update_successes= 0
    counter_mlwh_update_failures= 0

    for source_plate_barcode in source_plate_barcodes:
      logger.info(f"Processing source plate barcode {source_plate_barcode}")

      with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)

        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        # List[SampleDoc]
        sample_docs = get_samples_for_source_plate(samples_collection, source_plate_barcode)

        # iterate through samples
        current_source_plate_uuid = None
        for sample_doc in sample_docs:
          # will every sample doc have a plate_barcode and lab id?
          logger.info(f"Sample in well {sample_doc['coordinate']}")

          if current_source_plate_uuid == None:
            # extract lab id from sample doc
            lab_id = sample_doc[FIELD_MONGO_LAB_ID]
            logger.info(f"Creating a source_plate collection row with lab id = {lab_id}")
            # create source_plate record and extract lh_source_plate_uuid for next samples
            current_source_plate_uuid = create_mongo_source_plate_record(mongo_db, source_plate_barcode, lab_id)

          sample_doc[FIELD_LH_SOURCE_PLATE_UUID] = current_source_plate_uuid
          # generate an lh_sample_uuid if the sample doesn't have one
          if not sample_doc[FIELD_LH_SAMPLE_UUID]:
              sample_doc[FIELD_LH_SAMPLE_UUID] = str(uuid4())

          # update sample in Mongo ‘samples’ to set lh_source_plate uuid, lh_sample_uuid, and updated_timestamp
          try:
              success = update_mongo_sample_uuid_and_source_plate_uuid(samples_collection, sample_doc)
              if success:
                  counter_mongo_update_successes += 1
              else:
                  counter_mongo_update_failures += 1

          except Exception as e:
              counter_mongo_update_failures += 1
              logger.critical("Failed to update sample in Mongo for mongo id " f"{sample_doc[FIELD_MONGODB_ID]}")
              logger.exception(e)

          # update sample in MLWH 'lighthouse_samples' to set lh_source_plate, lh_sample_uuid, and updated_timestamp
          try:
              success = update_mlwh_sample_uuid_and_source_plate_uuid(config, sample_doc)
              if success:
                  counter_mlwh_update_successes += 1
              else:
                  counter_mlwh_update_failures += 1
          except Exception as e:
              counter_mlwh_update_failures += 1
              logger.critical("Failed to update sample in MLWH for mongo id " f"{sample_doc[FIELD_MONGODB_ID]}")
              logger.exception(e)

    logger.info(f"Count of successful Mongo updates = {counter_mongo_update_successes}")
    logger.info(f"Count of failed Mongo updates = {counter_mongo_update_failures}")
    logger.info(f"Count of successful MLWH updates = {counter_mlwh_update_successes}")
    logger.info(f"Count of failed MLWH updates = {counter_mlwh_update_failures}")

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
                    FIELD_UPDATED_AT: datetime.utcnow(),
                }
            },
        )
        if mongo_sample == None:
          return False
        else:
          return True

    except Exception as e:
        logger.critical("Failed to update sample in mongo for mongo id " f"{sample_doc[FIELD_MONGODB_ID]}")
        logger.exception(e)


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
        run_mysql_executemany_query(mysql_conn, SQL_MLWH_UPDATE_SAMPLE_UUID_PLATE_UUID, sample_doc)
        return True
    else:
        return False


def create_mongo_source_plate_record(mongo_db: Database, source_plate_barcode: str, lab_id: str) -> str:
    """Creates a mongo source_plate collection row

    Arguments:
        mongo_db {Database} -- the mongo database connection
        source_plate_barcode {str} -- the barcode of the source plate
        lab_id {str} -- the lab id for the sample

    Returns:
        bool -- whether the updates completed successfully
    """
    try:
        source_plates_collection = get_mongo_collection(mongo_db, COLLECTION_SOURCE_PLATES)

        new_plate_doc = create_source_plate_doc(source_plate_barcode, lab_id)
        new_plate_uuid = new_plate_doc[FIELD_LH_SOURCE_PLATE_UUID]

        logger.debug(f"Attempting to insert new source plate for barcode {source_plate_barcode} and lab id {lab_id}")
        source_plates_collection.insert_one(new_plate_doc)

        return new_plate_uuid

    except Exception as e:
        logger.critical(f"Error inserting a source plate row for barcode {source_plate_barcode} and lab id {lab_id}")
        logger.exception(e)


def get_samples_for_source_plate(samples_collection: Collection, source_plate_barcode: str) -> List[SampleDoc]:
    """Fetches the mongo samples collection rows for a given plate barcode

    Arguments:
        samples_collection {Collection} -- the mongo samples collection
        source_plate_barcode {str} -- the barcode of the source plate

    Returns:
        List[SampleDoc] -- the list of samples for the plate barcode
    """
    logger.debug(f"Selecting samples for source plate {source_plate_barcode}")

    match = {
        "$match": {
            # Filter by the plate barcode
            FIELD_PLATE_BARCODE: source_plate_barcode
        }
    }

    return list(samples_collection.aggregate([match]))

