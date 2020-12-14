import logging
import uuid
from datetime import datetime
from typing import List, Set, Tuple

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
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_UPDATED_AT,
    MONGO_DATETIME_FORMAT,
)
from crawler.db import (
    create_mongo_client,
    create_mysql_connection,
    get_mongo_collection,
    get_mongo_db,
    run_mysql_executemany_query,
)
from crawler.helpers.general_helpers import map_mongo_doc_to_sql_columns
from crawler.sql_queries import SQL_MLWH_MULTIPLE_INSERT
from crawler.types import Sample, SourcePlate
from migrations.helpers.shared_helper import valid_datetime_string, get_cherrypicked_samples
from migrations.helpers.update_filtered_positives_helper import update_dart_fields, update_filtered_positive_fields
from pymongo.collection import Collection
from pymongo.operations import UpdateOne

##
# Requirements:
# * Do not add samples to DART which have already been cherry-picked
# * Only add positive samples to DART
####
# 1. get samples from mongo between these time ranges that are positive
# 2. of these, find which have been cherry-picked and remove them from the list
# 3. add the UUID fields if not present
# 4. update samples in mongo updated in either of the above two steps (would expect the same set of samples from both
#       steps)
# 5. update the MLWH (should be an idempotent operation)
# 6. add all the plates of the positive samples we've selected in step 1 above, to DART


logger = logging.getLogger(__name__)


def migrate_all_dbs(config, s_start_datetime: str = "", s_end_datetime: str = "") -> None:
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

            # 1. get samples from mongo between these time ranges that are positive
            samples = get_positive_samples(samples_collection, start_datetime, end_datetime)

            if not samples:
                logger.info("No samples in this time range.")
                return

            logger.debug(f"{len(samples)} samples to process")

            root_sample_ids, plate_barcodes = extract_required_cp_info(samples)

            logger.debug(f"{len(plate_barcodes)} unique plate barcodes")

            # 2. of these, find which have been cherry-picked and remove them from the list
            cp_samples_df = get_cherrypicked_samples(config, list(root_sample_ids), list(plate_barcodes))

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
            mongo_docs_for_sql.append(map_mongo_doc_to_sql_columns(sample))

        if (num_sql_docs := len(mongo_docs_for_sql)) > 0:
            logger.info(f"Updating MLWH database for {num_sql_docs} sample documents")
            # create connection to the MLWH database
            with create_mysql_connection(config, False) as mlwh_conn:
                # 5. update the MLWH (should be an idempotent operation)
                run_mysql_executemany_query(mlwh_conn, SQL_MLWH_MULTIPLE_INSERT, mongo_docs_for_sql)

            # 6. add all the plates of the positive samples we've selected in step 1 above, to DART
            update_dart_fields(config, samples)
        else:
            logger.info("No documents found for this timestamp range, nothing to insert or update in MLWH or DART")
    except Exception as e:
        logger.error("Error while attempting to migrate all DBs")
        logger.exception(e)


def extract_required_cp_info(samples: List[Sample]) -> Tuple[Set[str], Set[str]]:
    root_sample_ids = set()
    plate_barcodes = set()

    for sample in samples:
        root_sample_ids.add(sample[FIELD_ROOT_SAMPLE_ID])
        plate_barcodes.add(sample[FIELD_PLATE_BARCODE])

    return root_sample_ids, plate_barcodes


def get_positive_samples(
    samples_collection: Collection, start_datetime: datetime, end_datetime: datetime
) -> List[Sample]:
    logger.debug(f"Selecting positive samples between {start_datetime} and {end_datetime}")

    match = {
        "$match": {
            # 1. First filter by the start and end dates
            FIELD_CREATED_AT: {"$gte": start_datetime, "$lte": end_datetime},
            FIELD_RESULT: {"$regex": "^positive", "$options": "i"},
        }
    }

    return list(samples_collection.aggregate([match]))


def remove_cherrypicked_samples(samples: List[Sample], cherry_picked_samples: List[List[str]]) -> List[Sample]:
    """Remove samples that have been cherry-picked. We need to check on (root sample id, plate barcode) combo rather
    than just root sample id. As multiple samples can exist with the same root sample id, with the potential for one
    being cherry-picked, and one not.

    Args:
        samples (List[Sample]): List of samples in the shape of mongo documents
        cherry_picked_samples (List[List[str]]): 2 dimensional list of cherry-picked samples with root sample id and
        plate barcodes for each.

    Returns:
        List[Sample]: The original list of samples minus the cherry-picked samples.
    """
    cherry_picked_sets = [{cp_sample[0], cp_sample[1]} for cp_sample in cherry_picked_samples]
    return list(
        filter(
            lambda sample: {sample[FIELD_ROOT_SAMPLE_ID], sample[FIELD_PLATE_BARCODE]} not in cherry_picked_sets,
            samples,
        )
    )


def add_sample_uuid_field(samples: List[Sample]) -> List[Sample]:
    for sample in samples:
        if FIELD_LH_SAMPLE_UUID not in [*sample]:
            sample[FIELD_LH_SAMPLE_UUID] = str(uuid.uuid4())

    return samples


def update_mongo_fields(mongo_db, samples: List[Sample]) -> bool:
    """Bulk updates sample filtered positive fields in the Mongo database

    Arguments:
        config {ModuleType} -- application config specifying database details
        samples {List[Sample]} -- the list of samples whose filtered positive fields should be updated

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


def samples_updated_with_source_plate_uuids(mongo_db, samples: List[Sample]) -> List[Sample]:
    logger.debug("Attempting to update docs with source plate UUIDs")

    updated_samples: List[Sample] = []

    def update_doc_from_source_plate(sample: Sample, existing_plate: SourcePlate, skip_lab_check: bool = False) -> None:
        if skip_lab_check or sample[FIELD_LAB_ID] == existing_plate[FIELD_LAB_ID]:
            sample[FIELD_LH_SOURCE_PLATE_UUID] = existing_plate[FIELD_LH_SOURCE_PLATE_UUID]
            updated_samples.append(sample)
        else:
            logger.error(
                f"ERROR: Source plate barcode {sample[FIELD_PLATE_BARCODE]} already exists with different lab_id "
                f"{existing_plate[FIELD_LAB_ID]}",
            )

    try:
        new_plates: List[SourcePlate] = []
        source_plates_collection = get_mongo_collection(mongo_db, COLLECTION_SOURCE_PLATES)

        for sample in samples:
            plate_barcode = sample[FIELD_PLATE_BARCODE]

            # attempt an update from plates that exist in mongo
            existing_mongo_plate = source_plates_collection.find_one({FIELD_BARCODE: plate_barcode})
            if existing_mongo_plate is not None:
                update_doc_from_source_plate(sample, existing_mongo_plate)
                continue

            # then add a new plate
            new_plate = new_mongo_source_plate(plate_barcode, sample[FIELD_LAB_ID])
            new_plates.append(new_plate)
            update_doc_from_source_plate(sample, new_plate, True)

        logger.debug(f"Attempting to insert {len(new_plates)} new source plates")
        if len(new_plates) > 0:
            source_plates_collection.insert_many(new_plates, ordered=False)

    except Exception:
        logger.error("Failed assigning source plate UUIDs to samples.")
        raise

    return updated_samples


def new_mongo_source_plate(plate_barcode: str, lab_id: str) -> SourcePlate:
    """Creates a new mongo source plate document.

    Arguments:
        plate_barcode {str} -- The plate barcode to assign to the new source plate.
        lab_id {str} -- The lab id to assign to the new source plate.

    Returns:
        SourcePlate -- The new mongo source plate doc.
    """
    timestamp = datetime.now()
    return {
        FIELD_LH_SOURCE_PLATE_UUID: str(uuid.uuid4()),
        FIELD_BARCODE: plate_barcode,
        FIELD_LAB_ID: lab_id,
        FIELD_UPDATED_AT: timestamp,
        FIELD_CREATED_AT: timestamp,
    }
