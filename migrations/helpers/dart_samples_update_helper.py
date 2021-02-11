import logging
import uuid
from datetime import datetime
from typing import List

from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.operations import UpdateOne

from crawler.constants import (
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    FIELD_BARCODE,
    FIELD_CREATED_AT,
    FIELD_LAB_ID,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_UPDATED_AT,
    MONGO_DATETIME_FORMAT,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.db.mysql import create_mysql_connection, run_mysql_executemany_query
from crawler.helpers.general_helpers import map_mongo_sample_to_mysql
from crawler.sql_queries import SQL_MLWH_MULTIPLE_INSERT
from crawler.types import Config, SampleDoc, SourcePlateDoc
from migrations.helpers.shared_helper import (
    extract_required_cp_info,
    get_cherrypicked_samples,
    remove_cherrypicked_samples,
    valid_datetime_string,
)
from migrations.helpers.update_filtered_positives_helper import update_dart_fields

##
# Requirements:
# * Do not add samples to DART which have already been cherry-picked
# * Only add positive samples to DART
####
# 1. get samples from mongo between these time ranges
# 2. of these, find which have been cherry-picked and remove them from the list
# 3. add the UUID fields if not present
# 4. update samples in mongo updated in either of the above two steps (would expect the same set of samples from both
#       steps)
# 5. update the MLWH (should be an idempotent operation)
# 6. add all the plates with non-cherrypicked samples (determined in step 2) to DART, as well as any positive samples
#       in these plates


logger = logging.getLogger(__name__)


def migrate_all_dbs(config: Config, s_start_datetime: str = "", s_end_datetime: str = "") -> None:
    if not config:
        logger.error("Aborting run: Config required")
        return

    if not valid_datetime_string(s_start_datetime):
        logger.error("Aborting run: Expected format of Start datetime is YYMMDD_HHmm")
        return

    if not valid_datetime_string(s_end_datetime):
        logger.error("Aborting run: Expected format of End datetime is YYMMDD_HHmm")
        return

    start_datetime = datetime.strptime(s_start_datetime, MONGO_DATETIME_FORMAT)
    end_datetime = datetime.strptime(s_end_datetime, MONGO_DATETIME_FORMAT)

    if start_datetime > end_datetime:
        logger.error("Aborting run: End datetime must be greater than Start datetime")
        return

    logger.info(f"Starting DART update process with Start datetime {start_datetime} and End datetime {end_datetime}")

    try:
        mongo_docs_for_sql = []

        # open connection to mongo
        with create_mongo_client(config) as client:
            mongo_db = get_mongo_db(config, client)

            samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

            # 1. get samples from mongo between these time ranges
            samples = get_samples(samples_collection, start_datetime, end_datetime)

            if not samples:
                logger.info("No samples in this time range.")
                return

            logger.debug(f"{len(samples)} samples to process")

            root_sample_ids, plate_barcodes = extract_required_cp_info(samples)

            logger.debug(f"{len(plate_barcodes)} unique plate barcodes")

            # 2. of these, find which have been cherry-picked and remove them from the list
            cp_samples_df = get_cherrypicked_samples(config, list(root_sample_ids), list(plate_barcodes))

            if cp_samples_df is None:  # we need to check if it is None explicitly
                raise Exception("Unable to determine cherry-picked sample - potentially error connecting to MySQL")

            # get the samples between those dates minus the cherry-picked ones
            if cp_samples_df is not None and not cp_samples_df.empty:
                # we need a list of cherry-picked samples with their respective plate barcodes
                cp_samples = cp_samples_df[[FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE]].to_numpy().tolist()

                logger.debug(f"{len(cp_samples)} cherry-picked samples in this timeframe")

                samples = remove_cherrypicked_samples(samples, cp_samples)
            else:
                logger.debug("No cherry-picked samples in this timeframe")

            logger.info(f"{len(samples)} samples between these timestamps and not cherry-picked")

            # 3. add the UUID fields if not present
            add_sample_uuid_field(samples)

            # update the samples with source plate UUIDs
            samples_updated_with_source_plate_uuids(mongo_db, samples)

            # 4. update samples in mongo updated in either of the above two steps (would expect the same set of samples
            #       from both steps)
            logger.info("Updating Mongo...")
            _ = update_mongo_fields(mongo_db, samples)
            logger.info("Finished updating Mongo")

        # convert mongo field values into MySQL format
        for sample in samples:
            mongo_docs_for_sql.append(map_mongo_sample_to_mysql(sample, copy_date=True))

        if (num_sql_docs := len(mongo_docs_for_sql)) > 0:
            logger.info(f"Updating MLWH database for {num_sql_docs} sample documents")
            # create connection to the MLWH database
            with create_mysql_connection(config, False) as mlwh_conn:
                # 5. update the MLWH (should be an idempotent operation)

                # Check here would migration dbs be ok?
                run_mysql_executemany_query(mlwh_conn, SQL_MLWH_MULTIPLE_INSERT, mongo_docs_for_sql)

            # 6. add all the plates with non-cherrypicked samples (determined in step 2) to DART, as well as any
            #       positive samples in these plates
            update_dart_fields(config, samples)
        else:
            logger.info("No documents found for this timestamp range, nothing to insert or update in MLWH or DART")
    except Exception as e:
        logger.error("Error while attempting to migrate all DBs")
        logger.exception(e)


def get_samples(samples_collection: Collection, start_datetime: datetime, end_datetime: datetime) -> List[SampleDoc]:
    logger.debug(f"Selecting samples between {start_datetime} and {end_datetime}")

    match = {
        "$match": {
            # Filter by the start and end dates
            FIELD_CREATED_AT: {"$gte": start_datetime, "$lte": end_datetime},
        }
    }

    return list(samples_collection.aggregate([match]))


def add_sample_uuid_field(samples: List[SampleDoc]) -> List[SampleDoc]:
    for sample in samples:
        if FIELD_LH_SAMPLE_UUID not in [*sample]:
            sample[FIELD_LH_SAMPLE_UUID] = str(uuid.uuid4())

    return samples


def update_mongo_fields(mongo_db: Database, samples: List[SampleDoc]) -> bool:
    """Bulk updates sample uuid fields in the Mongo database

    Arguments:
        config {ModuleType} -- application config specifying database details
        samples {List[Sample]} -- the list of samples whose uuid fields should be updated

    Returns:
        bool -- whether the updates completed successfully
    """
    samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)
    samples_collection.bulk_write(
        [
            UpdateOne(
                {FIELD_MONGODB_ID: sample[FIELD_MONGODB_ID]},
                {
                    "$set": {
                        FIELD_LH_SAMPLE_UUID: sample[FIELD_LH_SAMPLE_UUID],
                        FIELD_LH_SOURCE_PLATE_UUID: sample[FIELD_LH_SOURCE_PLATE_UUID],
                    }
                },
            )
            for sample in samples
        ]
    )
    return True


def samples_updated_with_source_plate_uuids(mongo_db: Database, samples: List[SampleDoc]) -> List[SampleDoc]:
    logger.debug("Attempting to update docs with source plate UUIDs")

    updated_samples: List[SampleDoc] = []

    def update_doc_from_source_plate(
        sample: SampleDoc, existing_plate: SourcePlateDoc, skip_lab_check: bool = False
    ) -> None:
        if skip_lab_check or sample[FIELD_LAB_ID] == existing_plate[FIELD_LAB_ID]:
            sample[FIELD_LH_SOURCE_PLATE_UUID] = existing_plate[FIELD_LH_SOURCE_PLATE_UUID]
            updated_samples.append(sample)
        else:
            logger.error(
                f"ERROR: Source plate barcode {sample[FIELD_PLATE_BARCODE]} already exists with different lab_id "
                f"{existing_plate[FIELD_LAB_ID]}",
            )

    try:
        new_plates: List[SourcePlateDoc] = []
        source_plates_collection = get_mongo_collection(mongo_db, COLLECTION_SOURCE_PLATES)

        for sample in samples:
            plate_barcode = sample[FIELD_PLATE_BARCODE]

            # attempt an update from plates that exist in mongo
            existing_mongo_plate = source_plates_collection.find_one({FIELD_BARCODE: plate_barcode})
            if existing_mongo_plate is not None:
                update_doc_from_source_plate(sample, existing_mongo_plate)
                continue

            # then add a new plate
            new_plate = new_mongo_source_plate(str(plate_barcode), str(sample[FIELD_LAB_ID]))
            new_plates.append(new_plate)
            update_doc_from_source_plate(sample, new_plate, True)

        logger.debug(f"Attempting to insert {len(new_plates)} new source plates")
        if len(new_plates) > 0:
            source_plates_collection.insert_many(new_plates, ordered=False)

    except Exception:
        logger.error("Failed assigning source plate UUIDs to samples.")
        raise

    return updated_samples


def new_mongo_source_plate(plate_barcode: str, lab_id: str) -> SourcePlateDoc:
    """Creates a new mongo source plate document.

    Arguments:
        plate_barcode {str} -- The plate barcode to assign to the new source plate.
        lab_id {str} -- The lab id to assign to the new source plate.

    Returns:
        SourcePlate -- The new mongo source plate doc.
    """
    timestamp = datetime.utcnow()
    return {
        FIELD_LH_SOURCE_PLATE_UUID: str(uuid.uuid4()),
        FIELD_BARCODE: plate_barcode,
        FIELD_LAB_ID: lab_id,
        FIELD_UPDATED_AT: timestamp,
        FIELD_CREATED_AT: timestamp,
    }
