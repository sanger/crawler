import logging
from datetime import datetime
from typing import Dict, List, Optional

from more_itertools import groupby_transform

from crawler.constants import (
    COLLECTION_SAMPLES,
    DART_STATE_PENDING,
    FIELD_COORDINATE,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SOURCE,
    POSITIVE_RESULT_VALUE,
)
from crawler.db import (
    add_dart_plate_if_doesnt_exist,
    create_dart_sql_server_conn,
    create_mongo_client,
    create_mysql_connection,
    get_mongo_collection,
    get_mongo_db,
    run_mysql_executemany_query,
    set_dart_well_properties,
)
from crawler.filtered_positive_identifier import FilteredPositiveIdentifier
from crawler.helpers.general_helpers import (
    get_dart_well_index,
    map_mongo_doc_to_dart_well_props,
    map_mongo_to_sql_common,
)
from crawler.sql_queries import SQL_DART_GET_PLATE_BARCODES, SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE
from crawler.types import Config, Sample
from migrations.helpers.shared_helper import extract_required_cp_info, get_cherrypicked_samples
from migrations.helpers.shared_helper import remove_cherrypicked_samples as remove_cp_samples

logger = logging.getLogger(__name__)


def pending_plate_barcodes_from_dart(config: Config) -> List[str]:
    """Fetch the barcodes of all plates from DART that are in the 'pending' state

    Arguments:
        config {Config} -- application config specifying database details

    Returns:
        List[str] -- barcodes of pending plates
    """
    sql_server_connection = create_dart_sql_server_conn(config)
    if sql_server_connection is None:
        # to be caught by calling method
        raise ValueError("Unable to establish DART SQL Server connection")

    plate_barcodes = []
    cursor = sql_server_connection.cursor()

    try:
        rows = cursor.execute(SQL_DART_GET_PLATE_BARCODES, DART_STATE_PENDING).fetchall()
        plate_barcodes = [row[0] for row in rows]
    except Exception as e:
        logger.error("Failed fetching pending plate barcodes from DART")
        logger.exception(e)
    finally:
        sql_server_connection.close()

    return plate_barcodes


def positive_result_samples_from_mongo(config: Config, plate_barcodes: Optional[List[str]] = None) -> List[Sample]:
    """Fetch positive samples from Mongo contained within specified plates.

    Arguments:
        config {Config} -- application config specifying database details
        plate_barcodes {Optional[List[str]]} -- barcodes of plates whose samples we are concerned with

    Returns:
        List[Dict[str, str]] -- List of positive samples contained within specified plates
    """
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        pipeline = [{"$match": {FIELD_RESULT: {"$eq": POSITIVE_RESULT_VALUE}}}]

        if plate_barcodes is not None:
            pipeline.append({"$match": {FIELD_PLATE_BARCODE: {"$in": plate_barcodes}}})  # type: ignore

        # this should take everything from the cursor find into RAM memory
        # (assuming you have enough memory)
        # should we project to an object that has fewer fields?
        return list(samples_collection.aggregate(pipeline))


def remove_cherrypicked_samples(config: Config, samples: List[Sample]) -> List[Sample]:
    """Filters an input list of samples for those that have not been cherrypicked.

    Arguments:
        config {Config} -- application config specifying database details
        samples {List[Sample]} -- the list of samples to filter

    Returns:
        List[Sample] -- non-cherrypicked samples
    """
    root_sample_ids, plate_barcodes = extract_required_cp_info(samples)
    cp_samples_df = get_cherrypicked_samples(config, list(root_sample_ids), list(plate_barcodes))

    if cp_samples_df is not None and not cp_samples_df.empty:
        cp_samples = cp_samples_df[[FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE]].to_numpy().tolist()
        return remove_cp_samples(samples, cp_samples)
    else:
        return samples


def update_filtered_positive_fields(
    filtered_positive_identifier: FilteredPositiveIdentifier,
    samples: List[Sample],
    version: str,
    update_timestamp: datetime,
) -> None:
    """Updates filtered positive fields on all passed-in sample documents - this method does not save the updates to
    the mongo database.

    Arguments:
        filtered_positive_identifier {FilteredPositiveIdentifier} -- the identifier through which to pass samples to,
        to determine whether they are filtered positive
        samples {List[Sample]} -- the list of samples for which to re-determine filtered positive values
        version {str} -- the filtered positive identifier version used
        update_timestamp {datetime} -- the timestamp at which the update was performed
    """
    logger.debug("Updating filtered positive fields")

    version = filtered_positive_identifier.version

    # Expect all samples to be passed into here to have a positive result
    for sample in samples:
        sample[FIELD_FILTERED_POSITIVE] = filtered_positive_identifier.is_positive(sample)
        sample[FIELD_FILTERED_POSITIVE_VERSION] = version
        sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] = update_timestamp


def update_mongo_filtered_positive_fields(
    config: Config, samples: List[Sample], version: str, update_timestamp: datetime
) -> bool:
    """Bulk updates sample filtered positive fields in the Mongo database

    Arguments:
        config {Config} -- application config specifying database details
        samples {List[Sample]} -- the list of samples whose filtered positive fields should be updated
        version {str} -- the filtered positive identifier version used
        update_timestamp {datetime} -- the timestamp at which the update was performed

    Returns:
        bool -- whether the updates completed successfully
    """
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)

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


def update_mlwh_filtered_positive_fields(config: Config, samples: List[Sample]) -> bool:
    """Bulk updates sample filtered positive fields in the MLWH database

    Arguments:
        config {Config} -- application config specifying database details
        samples {List[Dict[str, str]]} -- the list of samples whose filtered positive fields
        should be updated

    Returns:
        bool -- whether the updates completed successfully
    """
    mysql_conn = create_mysql_connection(config, False)

    if mysql_conn is not None and mysql_conn.is_connected():
        mlwh_samples = [map_mongo_to_sql_common(sample) for sample in samples]
        run_mysql_executemany_query(mysql_conn, SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE, mlwh_samples)
        return True
    else:
        return False


def update_dart_fields(config: Config, samples: List[Sample]) -> bool:
    """Updates DART plates and wells following updates to the filtered positive fields

    Arguments:
        config {Config} -- application config specifying database details
        samples {List[Dict[str, str]]} -- the list of samples to update in DART

    Returns:
        bool -- whether the updates completed successfully
    """
    sql_server_connection = create_dart_sql_server_conn(config)
    if sql_server_connection is None:
        raise ValueError("Unable to establish DART SQL Server connection")

    dart_updated_successfully = True
    labclass_by_centre_name = biomek_labclass_by_centre_name(config.CENTRES)
    try:
        cursor = sql_server_connection.cursor()

        for plate_barcode, samples_in_plate in groupby_transform(
            samples, lambda x: x[FIELD_PLATE_BARCODE], reducefunc=lambda x: list(x)  # type: ignore
        ):
            try:
                labware_class = labclass_by_centre_name[samples_in_plate[0][FIELD_SOURCE]]
                plate_state = add_dart_plate_if_doesnt_exist(
                    cursor, plate_barcode, labware_class  # type:ignore
                )
                if plate_state == DART_STATE_PENDING:
                    for sample in samples_in_plate:
                        well_index = get_dart_well_index(sample.get(FIELD_COORDINATE, None))
                        if well_index is not None:
                            well_props = map_mongo_doc_to_dart_well_props(sample)
                            set_dart_well_properties(
                                cursor, plate_barcode, well_props, well_index  # type:ignore
                            )
                        else:
                            raise ValueError(
                                "Unable to determine DART well index for sample "
                                f"{sample[FIELD_ROOT_SAMPLE_ID]} in plate {plate_barcode}"
                            )
                cursor.commit()
                dart_updated_successfully &= True
            except Exception as e:
                logger.error(f"Failed updating DART for samples in plate {plate_barcode}")
                logger.exception(e)
                cursor.rollback()
                dart_updated_successfully = False
    except Exception as e:
        logger.error("Failed updating DART")
        logger.exception(e)
        dart_updated_successfully = False
    finally:
        sql_server_connection.close()

    return dart_updated_successfully


def biomek_labclass_by_centre_name(centres: List[Dict[str, str]]) -> Dict[str, str]:
    """Determines a mapping between centre name and biomek labware class.

    Arguments:
        centres {List[Dict[str, str]]} -- the list of all centres

    Returns:
        Dict[str, str] -- biomek labware class by centre name
    """
    return {centre["name"]: centre["biomek_labware_class"] for centre in centres}
