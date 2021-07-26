import logging
from datetime import datetime
from typing import Any, Dict, List

from pandas import DataFrame

from crawler.constants import (
    COLLECTION_SAMPLES,
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
from crawler.types import Config, SampleDoc

logger = logging.getLogger(__name__)


def mongo_samples_by_date(config: Config, start_datetime: datetime, end_datetime: datetime) -> List[SampleDoc]:
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

        return num_versioned_samples > 0


def split_mongo_samples_by_version(
    samples: List[SampleDoc], cp_samples_df_v0: DataFrame, cp_samples_df_v1: DataFrame
) -> Dict[str, List[SampleDoc]]:
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
    config: Config, samples: List[SampleDoc], version: str, update_timestamp: datetime
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
