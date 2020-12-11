import logging
import uuid
from datetime import datetime
from types import ModuleType
from typing import List, Optional, Set, Tuple

import pandas as pd  # type: ignore
import sqlalchemy  # type: ignore
from crawler.constants import (
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    FIELD_BARCODE,
    FIELD_COORDINATE,
    FIELD_CREATED_AT,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    FIELD_FILTERED_POSITIVE_VERSION,
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
from migrations.helpers.shared_helper import valid_datetime_string
from migrations.helpers.update_filtered_positives_helper import update_dart_fields
from pandas import DataFrame
from pymongo.collection import Collection

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
        number_docs_found = 0

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
            _ = update_mongo_fields(mongo_db, samples, version, update_timestamp)
            logger.info("Finished updating Mongo")

        # 5. update the MLWH (should be an idempotent operation)
        # convert mongo field values into MySQL format
        for sample in samples:
            mongo_docs_for_sql.append(map_mongo_doc_to_sql_columns(sample))

        if number_docs_found > 0:
            logger.info(f"Updating MLWH database for {len(mongo_docs_for_sql)} sample documents")
            # create connection to the MLWH database
            with create_mysql_connection(config, False) as mlwh_conn:

                # TODO: make sure DART is not updated if this fails (https://github.com/sanger/crawler/issues/162)
                # execute SQL query to update filtered positive and UUID fields, it does this using the insert
                #   and performs an update when a duplicate key is found
                run_mysql_executemany_query(mlwh_conn, SQL_MLWH_MULTIPLE_INSERT, mongo_docs_for_sql)
        else:
            logger.info("No documents found for this timestamp range, nothing to insert or update in MLWH")

        # 6. add all the plates of the positive samples we've selected in step 1 above, to DART
        update_dart_fields(config, samples)
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


def update_mongo_fields(mongo_db, samples: List[Sample], version: str, update_timestamp: datetime) -> bool:
    """Bulk updates sample filtered positive fields in the Mongo database

    Arguments:
        config {ModuleType} -- application config specifying database details
        samples {List[Sample]} -- the list of samples whose filtered positive fields should be updated
        version {str} -- the filtered positive identifier version used
        update_timestamp {datetime} -- the timestamp at which the update was performed

    Returns:
        bool -- whether the updates completed successfully
    """
    # get ids of those that are filtered positive, and those that aren't
    all_ids: List[str] = [sample[FIELD_MONGODB_ID] for sample in samples]
    filtered_positive_ids: List[str] = [
        sample[FIELD_MONGODB_ID] for sample in list(filter(lambda x: x[FIELD_FILTERED_POSITIVE] is True, samples))
    ]
    filtered_negative_ids = [mongo_id for mongo_id in all_ids if mongo_id not in filtered_positive_ids]

    samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)
    samples_collection.update_many(
        {FIELD_MONGODB_ID: {"$in": filtered_positive_ids}},
        {
            "$set": {
                FIELD_FILTERED_POSITIVE: True,
                FIELD_FILTERED_POSITIVE_VERSION: version,
                FIELD_FILTERED_POSITIVE_TIMESTAMP: update_timestamp,
            }
        },
    )
    samples_collection.update_many(
        {FIELD_MONGODB_ID: {"$in": filtered_negative_ids}},
        {
            "$set": {
                FIELD_FILTERED_POSITIVE: False,
                FIELD_FILTERED_POSITIVE_VERSION: version,
                FIELD_FILTERED_POSITIVE_TIMESTAMP: update_timestamp,
            }
        },
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

    except Exception as e:
        logger.error("Failed assigning source plate UUIDs to samples.")
        logger.exception(e)
        updated_samples = []

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


def get_cherrypicked_samples(
    config: ModuleType,
    root_sample_ids: List[str],
    plate_barcodes: List[str],
    chunk_size: int = 50000,
) -> Optional[DataFrame]:
    """Find which samples have been cherrypicked using MLWH & Events warehouse.
    Returns dataframe with 4 columns: those needed to uniquely identify the sample resulting
    dataframe only contains those samples that have been cherrypicked (those that have an entry
    for the relevant event type in the event warehouse)

    Args:
        root_sample_ids (List[str]): [description]
        plate_barcodes (List[str]): [description]
        chunk_size (int, optional): [description]. Defaults to 50000.

    Returns:
        DataFrame: [description]
    """
    try:
        logger.debug("Getting cherry-picked samples from MLWH")

        # Create an empty DataFrame to merge into
        concat_frame = pd.DataFrame()

        chunk_root_sample_ids = [
            root_sample_ids[x : (x + chunk_size)] for x in range(0, len(root_sample_ids), chunk_size)  # noqa:E203
        ]

        sql_engine = sqlalchemy.create_engine(
            (
                f"mysql+pymysql://{config.MLWH_DB_RO_USER}:{config.MLWH_DB_RO_PASSWORD}"  # type: ignore
                f"@{config.MLWH_DB_HOST}"  # type: ignore
            ),
            pool_recycle=3600,
        )
        db_connection = sql_engine.connect()

        ml_wh_db = config.MLWH_DB_DBNAME  # type: ignore
        events_wh_db = config.EVENTS_WH_DB  # type: ignore

        for chunk_root_sample_id in chunk_root_sample_ids:
            sql = (
                f"SELECT mlwh_sample.description as `{FIELD_ROOT_SAMPLE_ID}`, mlwh_stock_resource.labware_human_barcode as `{FIELD_PLATE_BARCODE}`"  # noqa: E501
                f",mlwh_sample.phenotype as `Result_lower`, mlwh_stock_resource.labware_coordinate as `{FIELD_COORDINATE}`"  # noqa: E501
                f" FROM {ml_wh_db}.sample as mlwh_sample"
                f" JOIN {ml_wh_db}.stock_resource mlwh_stock_resource ON (mlwh_sample.id_sample_tmp = mlwh_stock_resource.id_sample_tmp)"  # noqa: E501
                f" JOIN {events_wh_db}.subjects mlwh_events_subjects ON (mlwh_events_subjects.friendly_name = sanger_sample_id)"  # noqa: E501
                f" JOIN {events_wh_db}.roles mlwh_events_roles ON (mlwh_events_roles.subject_id = mlwh_events_subjects.id)"  # noqa: E501
                f" JOIN {events_wh_db}.events mlwh_events_events ON (mlwh_events_roles.event_id = mlwh_events_events.id)"  # noqa: E501
                f" JOIN {events_wh_db}.event_types mlwh_events_event_types ON (mlwh_events_events.event_type_id = mlwh_events_event_types.id)"  # noqa: E501
                f" WHERE mlwh_sample.description IN %(root_sample_ids)s"
                f" AND mlwh_stock_resource.labware_human_barcode IN %(plate_barcodes)s"
                " AND mlwh_events_event_types.key = 'cherrypick_layout_set'"
                " GROUP BY mlwh_sample.description, mlwh_stock_resource.labware_human_barcode, mlwh_sample.phenotype, mlwh_stock_resource.labware_coordinate"  # noqa: E501
            )

            frame = pd.read_sql(
                sql,
                db_connection,
                params={
                    "root_sample_ids": tuple(chunk_root_sample_id),
                    "plate_barcodes": tuple(plate_barcodes),
                },
            )

            # drop_duplicates is needed because the same 'root sample id' could pop up in two different batches,
            # and then it would retrieve the same rows for that root sample id twice
            # do reset_index after dropping duplicates to make sure the rows are numbered in a way that makes sense
            concat_frame = concat_frame.append(frame).drop_duplicates().reset_index(drop=True)

        return concat_frame
    except Exception as e:
        logger.error("Error while connecting to MySQL")
        logger.exception(e)
        return None
    finally:
        db_connection.close()
