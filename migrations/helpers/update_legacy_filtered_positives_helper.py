from types import ModuleType
from typing import List, Optional, Set, Tuple
from pandas import DataFrame # type: ignore
import pandas as pd
import sqlalchemy  # type: ignore
from crawler.types import Sample
from crawler.constants import (
    COLLECTION_SAMPLES,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    V0_V1_CUTOFF_DATE,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_PLATE_BARCODE,
    FIELD_COORDINATE,
)
from crawler.db import (
    create_mongo_client,
    get_mongo_collection,
    get_mongo_db,
)
import logging

logger = logging.getLogger(__name__)


def filtered_positive_fields_exist(config: ModuleType):
    """Determines whether filtered positive fields exist in database

    Arguments:
        None

    Returns:
        Boolean -- Filtered positive fields exist
    """
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        return list(
            samples_collection.find(
                {
                    "$or": [
                        {FIELD_FILTERED_POSITIVE: {"$exists": True}},
                        {FIELD_FILTERED_POSITIVE_VERSION: {"$exists": True}},
                        {FIELD_FILTERED_POSITIVE_TIMESTAMP: {"$exists": True}},
                    ]
                }
            )
        )


def unmigrated_mongo_samples(config: ModuleType):
    """Gets all samples from Mongo which have not had the filtered positive field set

    Arguments:
        None

    Returns:
        List[Dict] -- All unmigrated samples from Mongo
    """
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        return list(
            samples_collection.find(
                {
                    "$and": [
                        {FIELD_FILTERED_POSITIVE: {"$exists": False}},
                        {FIELD_FILTERED_POSITIVE_VERSION: {"$exists": False}},
                        {FIELD_FILTERED_POSITIVE_TIMESTAMP: {"$exists": False}},
                    ]
                }
            )
        )


def get_v0_cherrypicked_samples(
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
                f" FROM {ml_wh_db}.sample as mlwh_sample"
                f" JOIN {ml_wh_db}.stock_resource mlwh_stock_resource ON (mlwh_sample.id_sample_tmp = mlwh_stock_resource.id_sample_tmp)"  # noqa: E501
                f" JOIN {events_wh_db}.subjects mlwh_events_subjects ON (mlwh_events_subjects.friendly_name = sanger_sample_id)"  # noqa: E501
                f" JOIN {events_wh_db}.roles mlwh_events_roles ON (mlwh_events_roles.subject_id = mlwh_events_subjects.id)"  # noqa: E501
                f" JOIN {events_wh_db}.events mlwh_events_events ON (mlwh_events_roles.event_id = mlwh_events_events.id)"  # noqa: E501
                f" JOIN {events_wh_db}.event_types mlwh_events_event_types ON (mlwh_events_events.event_type_id = mlwh_events_event_types.id)"  # noqa: E501
                f" WHERE mlwh_sample.description IN %(root_sample_ids)s"
                f" AND mlwh_sample.created <= '{V0_V1_CUTOFF_DATE}'"
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


def v0_version_set(config: ModuleType):
    """Find if v0 version has been set in any of the samples. This would indicate that the legacy migration has already been run.

    Args:
        samples {List[Dict]} -- List of samples from Mongo

    Returns:
        Boolean -- v0 version set in samples
    """
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        v0_samples = list(
            samples_collection.find(
                {"$and": [{FIELD_FILTERED_POSITIVE_VERSION: {"$exists": True}}, {"filtered_positive_version": "v0"}]}
            )
        )

        if len(v0_samples) > 0:
            return True
        else:
            return False


def split_v0_cherrypicked_mongo_samples(samples: List[Sample], cp_samples_df: DataFrame):
    """Split the Mongo samples dataframe based on the v0 cherrypicked samples. Samples
       which have been v0 cherrypicked need to have the v0 filtered postive rules
       applied. The remaining samples need the v1 rule applied.
    
    Args:
        samples {List[Sample]} -- List of samples from Mongo

    Returns:
        v0_unmigrated_samples {DataFrame} -- Mongo samples to be updated with v0 rules
        v1_unmigrated_samples {DataFrame} -- Mongo samples to be updated with v1 rules
    """
