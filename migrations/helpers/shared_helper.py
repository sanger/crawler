import logging
import sys
import traceback
from datetime import datetime
from typing import Dict, Iterator, List, Optional, Set, Tuple

import pandas as pd
import sqlalchemy
from pandas import DataFrame

from crawler.constants import FIELD_PLATE_BARCODE, FIELD_ROOT_SAMPLE_ID, MONGO_DATETIME_FORMAT
from crawler.sql_queries import SQL_MLWH_GET_CP_SAMPLES, SQL_MLWH_GET_CP_SAMPLES_BY_DATE
from crawler.types import Config, SampleDoc

logger = logging.getLogger(__name__)


def print_exception() -> None:
    print(f"An exception occurred, at {datetime.now()}")
    e = sys.exc_info()
    print(e[0])  # exception type
    print(e[1])  # exception message
    if e[2]:  # traceback
        traceback.print_tb(e[2], limit=10)


def valid_datetime_string(s_datetime: Optional[str]) -> bool:
    """Validates a string against the mongo datetime format.

    Arguments:
        s_datetime (str): string of date to validate

    Returns:
        bool: True if the date is valid, False otherwise
    """
    if not s_datetime:
        return False

    try:
        datetime.strptime(s_datetime, MONGO_DATETIME_FORMAT)
        return True
    except Exception:
        print_exception()
        return False


def extract_required_cp_info(samples: List[SampleDoc]) -> Tuple[Set[str], Set[str]]:
    root_sample_ids = set()
    plate_barcodes = set()

    for sample in samples:
        root_sample_ids.add(str(sample[FIELD_ROOT_SAMPLE_ID]))
        plate_barcodes.add(str(sample[FIELD_PLATE_BARCODE]))

    return root_sample_ids, plate_barcodes


def params_for_cherrypicked_samples_query(
    root_sample_ids: List[str],
    plate_barcodes: List[str],
    chunk_size: int = 50000,
) -> Iterator[Dict]:
    for x in range(0, len(root_sample_ids), chunk_size):
        yield {
            "root_sample_ids": tuple(root_sample_ids[x : (x + chunk_size)]),  # noqa: E203
            "plate_barcodes": plate_barcodes,
        }


def params_for_cherrypicked_samples_by_date_query(
    root_sample_ids: List[str],
    plate_barcodes: List[str],
    start_date: str,
    end_date: str,
    chunk_size: int = 50000,
) -> Iterator[Dict]:
    for params in params_for_cherrypicked_samples_query(root_sample_ids, plate_barcodes, chunk_size):
        yield {
            **params,
            **{
                "start_date": start_date,
                "end_date": end_date,
            },
        }


def build_data_frame_from_database_queries_aggregation(
    database_connection_uri: str, query_template: str, params_iterator: Iterator[Dict]
) -> Optional[DataFrame]:
    try:
        db_connection = None

        logger.debug("Getting cherry-picked samples from MLWH")

        # Create an empty DataFrame to merge into
        concat_frame = pd.DataFrame()

        sql_engine = sqlalchemy.create_engine(database_connection_uri, pool_recycle=3600)
        db_connection = sql_engine.connect()

        for params in params_iterator:
            data_frame = pd.read_sql(query_template, db_connection, params=params)
            # drop_duplicates is needed because the same 'root sample id' could pop up in two different batches,
            # and then it would retrieve the same rows for that root sample id twice
            # do reset_index after dropping duplicates to make sure the rows are numbered in a way that makes sense

            concat_frame = concat_frame.append(data_frame).drop_duplicates().reset_index(drop=True)

        return concat_frame
    except Exception as e:
        logger.error("Error while connecting to MySQL")
        logger.exception(e)
        return None
    finally:
        if db_connection:
            db_connection.close()


def database_connection_uri(config: Config) -> str:
    return (
        f"mysql+pymysql://{config.MLWH_DB_RO_USER}:{config.MLWH_DB_RO_PASSWORD}"
        f"@{config.MLWH_DB_HOST}:{config.MLWH_DB_PORT}/{config.MLWH_DB_DBNAME}"
    )


def get_cherrypicked_samples(
    config: Config,
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
    logger.debug("Getting cherry-picked samples from MLWH")

    return build_data_frame_from_database_queries_aggregation(
        database_connection_uri(config),
        SQL_MLWH_GET_CP_SAMPLES,
        params_for_cherrypicked_samples_query(root_sample_ids, plate_barcodes, chunk_size),
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

    logger.debug("Getting cherry-picked samples from MLWH")

    return build_data_frame_from_database_queries_aggregation(
        database_connection_uri(config),
        SQL_MLWH_GET_CP_SAMPLES_BY_DATE,
        params_for_cherrypicked_samples_by_date_query(
            root_sample_ids, plate_barcodes, start_date, end_date, chunk_size
        ),
    )


def remove_cherrypicked_samples(samples: List[SampleDoc], cherry_picked_samples: List[List[str]]) -> List[SampleDoc]:
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
