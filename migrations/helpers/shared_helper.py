from datetime import datetime
import logging
import pandas as pd  # type: ignore
from pandas import DataFrame
import sqlalchemy  # type: ignore
import sys
import traceback
from types import ModuleType
from typing import Optional, List, Tuple, Set

from crawler.constants import (
    FIELD_COORDINATE,
    FIELD_PLATE_BARCODE,
    FIELD_ROOT_SAMPLE_ID,
    MONGO_DATETIME_FORMAT,
)
from crawler.types import Sample


logger = logging.getLogger(__name__)


def print_exception() -> None:
    print(f"An exception occurred, at {datetime.now()}")
    e = sys.exc_info()
    print(e[0])  # exception type
    print(e[1])  # exception message
    if e[2]:  # traceback
        traceback.print_tb(e[2], limit=10)


def valid_datetime_string(s_datetime: str) -> bool:
    try:
        dt = datetime.strptime(s_datetime, MONGO_DATETIME_FORMAT)
        if dt is None:
            return False
        return True
    except Exception:
        print_exception()
        return False


def extract_required_cp_info(samples: List[Sample]) -> Tuple[Set[str], Set[str]]:
    root_sample_ids = set()
    plate_barcodes = set()

    for sample in samples:
        root_sample_ids.add(sample[FIELD_ROOT_SAMPLE_ID])
        plate_barcodes.add(sample[FIELD_PLATE_BARCODE])

    return root_sample_ids, plate_barcodes


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
        db_connection = None

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
            params = {"root_sample_ids": tuple(chunk_root_sample_id), "plate_barcodes": tuple(plate_barcodes)}

            sentinel_sql = __sentinel_cherrypicked_samples_query(ml_wh_db, events_wh_db)
            sentinel_frame = pd.read_sql(sentinel_sql, db_connection, params=params)

            # drop_duplicates is needed because the same 'root sample id' could pop up in two different batches,
            # and then it would retrieve the same rows for that root sample id twice
            # do reset_index after dropping duplicates to make sure the rows are numbered in a way that makes sense
            concat_frame = concat_frame.append(sentinel_frame).drop_duplicates().reset_index(drop=True)

            beckman_sql = __beckman_cherrypicked_samples_query(ml_wh_db, events_wh_db)
            beckman_frame = pd.read_sql(beckman_sql, db_connection, params=params)

            # again we concatenate dropping duplicates here (same reason as outlined above)
            concat_frame = (concat_frame.append(beckman_frame).drop_duplicates().reset_index(drop=True))

        return concat_frame
    except Exception as e:
        logger.error("Error while connecting to MySQL")
        logger.exception(e)
        return None
    finally:
        if db_connection:
            db_connection.close()


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


# Private, not explicitly tested methods

def __sentinel_cherrypicked_samples_query(ml_wh_db: str, events_wh_db: str) -> str:
    """Forms the SQL query to identify samples cherrypicked via the Sentinel workflow.

    Arguments:
        ml_wh_db {str} -- The name of the MLWH database
        events_wh_db {str} -- The name of the Events Warehouse database

    Returns:
        str -- the SQL query for Sentinel cherrypicked samples
    """
    return (
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


def __beckman_cherrypicked_samples_query(ml_wh_db: str, events_wh_db: str) -> str:
    """Forms the SQL query to identify samples cherrypicked via the Beckman workflow.

    Arguments:
        ml_wh_db {str} -- The name of the MLWH database
        events_wh_db {str} -- The name of the Events Warehouse database

    Returns:
        str -- the SQL query for Beckman cherrypicked samples
    """
    return (
        f"SELECT mlwh_sample.description AS `{FIELD_ROOT_SAMPLE_ID}`, mlwh_lh_sample.plate_barcode AS `{FIELD_PLATE_BARCODE}`,"  # noqa: E501
        f" mlwh_sample.phenotype AS `Result_lower`, mlwh_lh_sample.coordinate AS `{FIELD_COORDINATE}`"  # noqa: E501
        f" FROM {ml_wh_db}.sample AS mlwh_sample"
        f" JOIN {ml_wh_db}.lighthouse_sample AS mlwh_lh_sample ON (mlwh_sample.uuid_sample_lims = mlwh_lh_sample.lh_sample_uuid)"  # noqa: E501
        f" JOIN {events_wh_db}.subjects AS mlwh_events_subjects ON (mlwh_events_subjects.uuid = UNHEX(REPLACE(mlwh_lh_sample.lh_sample_uuid, '-', '')))"  # noqa: E501
        f" JOIN {events_wh_db}.roles AS mlwh_events_roles ON (mlwh_events_roles.subject_id = mlwh_events_subjects.id)"  # noqa: E501
        f" JOIN {events_wh_db}.events AS mlwh_events_events ON (mlwh_events_events.id = mlwh_events_roles.event_id)"  # noqa: E501
        f" JOIN {events_wh_db}.event_types AS mlwh_events_event_types ON (mlwh_events_event_types.id = mlwh_events_events.event_type_id)"  # noqa: E501
        f" WHERE mlwh_sample.description IN %(root_sample_ids)s"
        f" AND mlwh_lh_sample.plate_barcode IN %(plate_barcodes)s"
        f" AND mlwh_events_event_types.key = 'lh_beckman_cp_destination_created'"
        " GROUP BY mlwh_sample.description, mlwh_lh_sample.plate_barcode, mlwh_sample.phenotype, mlwh_lh_sample.coordinate;"  # noqa: E501
    )
