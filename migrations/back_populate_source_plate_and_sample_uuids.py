import csv
from datetime import datetime
import logging
import logging.config
import os
import stat
# from typing import Any, Dict, Final, Iterator, List, Optional, Set, Tuple, cast
from typing import List

# from typing import Any, Dict, Iterator, List, Tuple

# from bson.objectid import ObjectId
from pymongo.collection import Collection

from crawler.constants import (
    COLLECTION_SOURCE_PLATES,
    COLLECTION_SAMPLES,
    FIELD_MONGODB_ID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_LH_SAMPLE_UUID,
    FIELD_PLATE_BARCODE,
    FIELD_UPDATED_AT,
    MLWH_MONGODB_ID,
    MLWH_LH_SOURCE_PLATE_UUID,
    MLWH_LH_SAMPLE_UUID,
    MLWH_UPDATED_AT,
    MONGO_DATETIME_FORMAT,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
# from crawler.db.mysql import create_mysql_connection
# from crawler.db.mysql import insert_or_update_samples_in_mlwh
from crawler.helpers.general_helpers import create_source_plate_doc
from crawler.types import Config, SampleDoc, SourcePlateDoc

logger = logging.getLogger(__name__)

"""
Assumptions:
1. checked source plates do not already have lh_source_plate_uuid or lh_sample_uuid in either
the mongo 'source_plate' or 'samples' collections, or in the MLWH lighthouse_sample table
2. the samples do not have any duplicates for the same RNA Id in either mongo or MLWH

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


def validate_args(
    config: Config, s_filepath: str
) -> str:
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
  # TODO
  mode = os.lstat(s_filepath).st_mode
  return is_csv_file(mode, s_filepath)

def extract_barcodes(config: Config, filepath: str) -> List[str]:
  # TODO
  extracted_barcodes = ['GLS000001', 'GLS000002']
  return extracted_barcodes

def update_uuids_mongo_and_mlwh(config: Config, source_plate_barcodes: List[str]):
  for source_plate_barcode in source_plate_barcodes:
    logger.info(f"Processing source plate barcode {source_plate_barcode}")

  # TODO
  return

def is_csv_file(mode: int, file_name: str) -> bool:
    if stat.S_ISREG(mode):
        file_name, file_extension = os.path.splitext(file_name)
        return file_extension == ".csv"

    return False

def create_mongo_source_plate_record(source_plate_barcode: str, lab_id: str) -> str:
    try:
        source_plates_collection = get_mongo_collection(self.get_db(), COLLECTION_SOURCE_PLATES)

        new_plate_doc = create_source_plate_doc(source_plate_barcode, lab_id)
        new_plate_uuid = new_plate_doc[FIELD_LH_SOURCE_PLATE_UUID]

        logger.debug("Attempting to insert new source plate for barcode " f"{source_plate_barcode}")
        source_plates_collection.insert(new_plate_doc)

        return new_plate_uuid

    except Exception as e:
        logger.critical("Error inserting a source plate row for barcode " f"{source_plate_barcode}")
        logger.exception(e)

def get_samples_for_source_plate(samples_collection: Collection, source_plate_barcode: str) -> List[SampleDoc]:
    logger.debug(f"Selecting samples for source plate {source_plate_barcode}")

    match = {
        "$match": {
            # Filter by the plate barcode
            FIELD_PLATE_BARCODE: source_plate_barcode
        }
    }

    return list(samples_collection.aggregate([match]))

