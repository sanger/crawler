import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, cast

import pandas as pd
import sqlalchemy
from pandas import DataFrame

from crawler.constants import (
    COLLECTION_SAMPLES,
    EVENT_CHERRYPICK_LAYOUT_SET,
    FIELD_CREATED_AT,
    FIELD_FILTERED_POSITIVE,
    FIELD_PLATE_BARCODE,
    FIELD_ROOT_SAMPLE_ID,
    MLWH_FILTERED_POSITIVE,
    MLWH_MONGODB_ID,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.db.mysql import create_mysql_connection, run_mysql_execute_formatted_query
from crawler.filtered_positive_identifier import (
    FILTERED_POSITIVE_VERSION_0,
    FILTERED_POSITIVE_VERSION_1,
    FILTERED_POSITIVE_VERSION_2,
)
from crawler.helpers.general_helpers import map_mongo_to_sql_common
from crawler.sql_queries import SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE_BATCH
from crawler.types import Config, Sample

logger = logging.getLogger(__name__)


def mongo_samples_by_date(config: Config, start_datetime: datetime, end_datetime: datetime) -> List[Sample]:
    """Gets all samples from Mongo created before Crawler started setting filtered positive fields

    Arguments:
        config {Config} -- application config specifying database details
        start_datetime {datetime} -- lower limit of sample creation date
        end_datetime {datetime} -- upper limit of sample creation date
    Returns:
        List[Sample] -- List of Mongo samples created before filtered positive Crawler changes
    """
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)
        return list(
            samples_collection.find(
                {
                    FIELD_CREATED_AT: {"$gte": start_datetime, "$lt": end_datetime},
                }
            )
        )


def get_cherrypicked_samples_by_date(
    config: Config,
    root_sample_ids: List[str],
    plate_barcodes: List[str],
    start_date: str,
    end_date: str,
    chunk_size: int = 50000,
) -> Optional[DataFrame]:
    """Find which samples have been cherrypicked between defined dates using MLWH & Events warehouse.
    Returns dataframe with 4 columns: those needed to uniquely identify the sample resulting
    dataframe only contains those samples that have been cherrypicked (those that have an entry
    for the relevant event type in the event warehouse)

    Args:
        config (Config): application config specifying database details
        root_sample_ids (List[str]): [description]
        plate_barcodes (List[str]): [description]
        start_date (str): lower limit on creation date
        end_date (str): upper limit on creation date
        chunk_size (int, optional): [description]. Defaults to 50000.

    Returns:
        DataFrame: [description]
    """
    try:
        db_connection = None

        logger.debug("Getting cherry-picked samples from MLWH")

        # Create an empty DataFrame to merge into
        concat_frame = pd.DataFrame()

        chunk_root_sample_ids = [
            root_sample_ids[x : (x + chunk_size)] for x in range(0, len(root_sample_ids), chunk_size)  # noqa: E203
        ]

        sql_engine = sqlalchemy.create_engine(
            (
                f"mysql+pymysql://{config.MLWH_DB_RO_USER}:{config.MLWH_DB_RO_PASSWORD}"
                f"@{config.MLWH_DB_HOST}:{config.MLWH_DB_PORT}"
            ),
            pool_recycle=3600,
        )
        db_connection = sql_engine.connect()

        ml_wh_db = config.MLWH_DB_DBNAME
        events_wh_db = config.EVENTS_WH_DB

        values_index = 0
        for chunk_root_sample_id in chunk_root_sample_ids:
            logger.debug(f"Querying records between {values_index} and {values_index + chunk_size}")

            # Note we only querying for Sentinel cherrypicked samples as we expect the timestamps used in the query
            # to all be earlier than when the Beckman workflow was adopted
            sql = (
                f"SELECT mlwh_sample.description as `{FIELD_ROOT_SAMPLE_ID}`, mlwh_stock_resource.labware_human_barcode as `{FIELD_PLATE_BARCODE}`"  # noqa: E501
                f" FROM {ml_wh_db}.sample as mlwh_sample"
                f" JOIN {ml_wh_db}.stock_resource mlwh_stock_resource ON (mlwh_sample.id_sample_tmp = mlwh_stock_resource.id_sample_tmp)"  # noqa: E501
                f" JOIN {events_wh_db}.subjects mlwh_events_subjects ON (mlwh_events_subjects.friendly_name = sanger_sample_id)"  # noqa: E501
                f" JOIN {events_wh_db}.roles mlwh_events_roles ON (mlwh_events_roles.subject_id = mlwh_events_subjects.id)"  # noqa: E501
                f" JOIN {events_wh_db}.events mlwh_events_events ON (mlwh_events_roles.event_id = mlwh_events_events.id)"  # noqa: E501
                f" JOIN {events_wh_db}.event_types mlwh_events_event_types ON (mlwh_events_events.event_type_id = mlwh_events_event_types.id)"  # noqa: E501
                f" WHERE mlwh_sample.description IN %(root_sample_ids)s"
                f" AND mlwh_stock_resource.labware_human_barcode IN %(plate_barcodes)s"
                f" AND mlwh_sample.created >= '{start_date}'"
                f" AND mlwh_sample.created < '{end_date}'"
                f" AND mlwh_stock_resource.labware_human_barcode IN %(plate_barcodes)s"
                f" AND mlwh_events_event_types.key = '{EVENT_CHERRYPICK_LAYOUT_SET}'"
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

            # drop_duplicates is needed because the same 'root sample id' could
            # pop up in two different batches, and then it would
            # retrieve the same rows for that root sample id twice
            # do reset_index after dropping duplicates to make sure the rows are numbered
            # in a way that makes sense
            concat_frame = concat_frame.append(frame).drop_duplicates().reset_index(drop=True)
            values_index += chunk_size
        return concat_frame
    except Exception as e:
        logger.error("Error while connecting to MySQL")
        logger.exception(e)
        raise
    finally:
        if db_connection:
            db_connection.close()


def filtered_positive_fields_set(config: Config, start_datetime: datetime, end_datetime: datetime) -> bool:
    """Find if the filtered positive version field has been set on any of samples in date range.
       This would indicate that the migration has already been run on those samples.

    Args:
        config {Config} -- application config specifying database details
        start_datetime {datetime} -- lower limit of sample creation date
        end_datetime {datetime} -- upper limit of sample creation date

    Returns:
        {bool} -- v0 version set in samples
    """
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        num_versioned_samples = samples_collection.count_documents(
            {
                FIELD_CREATED_AT: {"$gte": start_datetime, "$lt": end_datetime},
                FIELD_FILTERED_POSITIVE: {"$exists": True},
            }
        )

        return cast(bool, num_versioned_samples > 0)


def split_mongo_samples_by_version(
    samples: List[Sample], cp_samples_df_v0: DataFrame, cp_samples_df_v1: DataFrame
) -> Dict[str, List[Sample]]:
    """Split the Mongo samples dataframe based on the v0 cherrypicked samples. Samples
       which have been v0 cherrypicked need to have the v0 filtered positive rules
       applied. The remaining samples need the v1 rule applied.

    Args:
        samples {List[Sample]} -- List of samples from Mongo
        cp_samples_df_v0 {DataFrame} -- DataFrame of v0 cherrypicked samples
        cp_samples_df_v1: {DataFrame} -- DataFrame of v1 cherrypicked samples

    Returns:
        samples_by_version {Dict[List[Sample]]} -- Samples split by version
    """
    v0_cp_samples = []
    if not cp_samples_df_v0.empty:
        v0_cp_samples = cp_samples_df_v0[[FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE]].to_numpy().tolist()  # noqa: E501

    v1_cp_samples = []
    if not cp_samples_df_v1.empty:
        v1_cp_samples = cp_samples_df_v1[[FIELD_ROOT_SAMPLE_ID, FIELD_PLATE_BARCODE]].to_numpy().tolist()  # noqa: E501

    v0_samples = []
    v1_samples = []
    v2_samples = []

    counter = 0
    for sample in samples:
        if [sample[FIELD_ROOT_SAMPLE_ID], sample[FIELD_PLATE_BARCODE]] in v0_cp_samples:
            v0_samples.append(sample)
        elif [sample[FIELD_ROOT_SAMPLE_ID], sample[FIELD_PLATE_BARCODE]] in v1_cp_samples:
            v1_samples.append(sample)
        else:
            v2_samples.append(sample)
        counter += 1

        if counter % 10000 == 0:
            logger.debug(f"Split {counter} samples by version")

    samples_by_version = {
        FILTERED_POSITIVE_VERSION_0: v0_samples,
        FILTERED_POSITIVE_VERSION_1: v1_samples,
        FILTERED_POSITIVE_VERSION_2: v2_samples,
    }

    return samples_by_version


def update_mlwh_filtered_positive_fields_batched(
    config: Config, samples: List[Sample], version: str, update_timestamp: datetime
) -> bool:
    """Bulk updates sample filtered positive fields in the MLWH database

    Arguments:
        config {Config} -- application config specifying database details
        samples {List[Dict[str, str]]} -- the list of samples whose filtered positive fields
        should be updated
        version {str} -- filtered positive version
        update_timestamp {datetime} -- time of filtered positive fields update

    Returns:
        bool -- whether the updates completed successfully
    """
    mysql_conn = create_mysql_connection(config, False)
    completed_successfully = False
    try:
        if mysql_conn is not None and mysql_conn.is_connected():
            num_samples = len(samples)
            ROWS_PER_QUERY = 15000
            samples_index = 0
            logger.debug(f"Attempting to update {num_samples} rows in the MLWH database in batches of {ROWS_PER_QUERY}")
            while samples_index < num_samples:
                samples_batch = samples[samples_index : (samples_index + ROWS_PER_QUERY)]  # noqa: E203
                mlwh_samples_batch = [map_mongo_to_sql_common(sample) for sample in samples_batch]

                filtered_positive_ids = []
                filtered_negative_ids = []
                for sample in mlwh_samples_batch:
                    if sample[MLWH_FILTERED_POSITIVE] is True:
                        filtered_positive_ids.append(sample[MLWH_MONGODB_ID])
                    else:
                        filtered_negative_ids.append(sample[MLWH_MONGODB_ID])

                filtered_positive_num = len(filtered_positive_ids)
                logger.info(f"Attempting to update {filtered_positive_num} {version} filtered positive samples in MLWH")

                if filtered_positive_num > 0:
                    positive_args: List[Any] = [True, version, update_timestamp, update_timestamp]
                    run_mysql_execute_formatted_query(
                        mysql_conn,
                        SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE_BATCH,
                        filtered_positive_ids,
                        positive_args,
                    )

                filtered_negative_num = len(filtered_negative_ids)
                logger.info(
                    f"Attempting to update {filtered_negative_num} {version} filtered positive false samples in MLWH"
                )

                if filtered_negative_num > 0:
                    negative_args: List[Any] = [False, version, update_timestamp, update_timestamp]
                    run_mysql_execute_formatted_query(
                        mysql_conn,
                        SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE_BATCH,
                        filtered_negative_ids,
                        negative_args,
                    )

                samples_index += ROWS_PER_QUERY
            completed_successfully = True
        return completed_successfully
    except Exception:
        logger.error("MLWH filtered positive field batched updates failed")
        raise
    finally:
        # close the connection
        logger.debug("Closing the MLWH database connection.")
        mysql_conn.close()
