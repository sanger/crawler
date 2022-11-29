import logging
import logging.config
from datetime import datetime
from typing import Any, List, cast

from mysql.connector.cursor_cext import CMySQLCursor
from pymongo.collection import Collection

from crawler.constants import COLLECTION_SAMPLES, FIELD_COORDINATE, FIELD_MONGODB_ID, FIELD_PLATE_BARCODE, FIELD_RNA_ID
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.db.mysql import create_mysql_connection
from crawler.sql_queries import SQL_MLWH_GET_BY_RNA_ID, SQL_MLWH_UPDATE_MONGODB_ID_BY_ID
from crawler.types import Config, SampleDoc
from migrations.helpers.shared_helper import extract_barcodes, validate_args

logger = logging.getLogger(__name__)


def run(config: Config, s_filepath: str) -> None:
    filepath = validate_args(config=config, s_filepath=s_filepath)

    logger.info("-" * 80)
    logger.info("STARTING RECONNECTING MLWH WITH MONGO")
    logger.info(f"Time start: {datetime.now()}")

    logger.info(f"Starting migration process with supplied file {filepath}")

    source_plate_barcodes = extract_barcodes(filepath=filepath)

    logger.info(f"Source plate barcodes {source_plate_barcodes}")
    reconnect_mlwh_with_mongo(config=config, source_plate_barcodes=source_plate_barcodes)


def reconnect_mlwh_with_mongo(config: Config, source_plate_barcodes: List[str]) -> None:
    """Updates MLWH to match mongo records by connecting with RNA ID

    Arguments:
        config {Config} -- application config specifying database details
        source_plate_barcodes {List[str]} -- the list of source plate barcodes

    Returns:
        Nothing
    """

    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        # check_samples_are_valid(config, samples_collection, source_plates_collection, source_plate_barcodes)

        for source_plate_barcode in source_plate_barcodes:
            logger.info(f"Processing source plate barcode {source_plate_barcode}")

            # List[SampleDoc]
            sample_docs = obtain_samples_for_source_plate(samples_collection, source_plate_barcode)

            # iterate through samples
            for sample_doc in sample_docs:
                # will every sample doc have a plate_barcode and lab id?
                logger.info(f"Sample in well {sample_doc[FIELD_COORDINATE]}")
                mlwh_row = mlwh_get_samples_by_rna_id(config, cast(str, sample_doc[FIELD_RNA_ID]))
                logger.info(
                    f"Altering lighthouse_sample with id={mlwh_row[0]}, "
                    f"changing mongodb_id from ={mlwh_row[1]} "
                    f"to ={sample_doc[FIELD_MONGODB_ID]}"
                )
                mlwh_update_sample_mongo_db_id(config, mlwh_row[0], cast(str, sample_doc[FIELD_MONGODB_ID]))


def obtain_samples_for_source_plate(samples_collection: Collection, source_plate_barcode: str) -> List[SampleDoc]:
    """Fetches the mongo samples collection rows for a given plate barcode

    Arguments:
        samples_collection {Collection} -- the mongo samples collection
        source_plate_barcode {str} -- the barcode of the source plate

    Returns:
        List[SampleDoc] -- the list of samples for the plate barcode
    """
    logger.debug(f"Selecting samples for source plate {source_plate_barcode}")

    match = {
        "$match": {
            # Filter by the plate barcode
            FIELD_PLATE_BARCODE: source_plate_barcode
        }
    }

    return list(samples_collection.aggregate([match]))


def mlwh_get_samples_by_rna_id(config: Config, rna_id: str) -> Any:
    """Count samples by RNA ID

    Arguments:
        config {Config} -- application config specifying database details
        rna_id {str} -- the rna id to find

    Returns:
        Any -- mysql record for the rna id
    """
    with create_mysql_connection(config, False) as mysql_conn:
        if mysql_conn is not None and mysql_conn.is_connected():
            cursor: CMySQLCursor = mysql_conn.cursor()
            query_str = SQL_MLWH_GET_BY_RNA_ID % {"rna_id": str(rna_id)}
            cursor.execute(query_str)
            rows = cursor.fetchall()
            if len(rows) > 1:
                raise Exception("More than 1 row obtained for the same RNA ID")

            return rows[0]
        else:
            raise Exception("Cannot connect mysql")


def mlwh_update_sample_mongo_db_id(config: Config, id: int, mongodb_id: str) -> None:
    with create_mysql_connection(config, False) as mysql_conn:
        if mysql_conn is not None and mysql_conn.is_connected():
            cursor: CMySQLCursor = mysql_conn.cursor()
            query_str = SQL_MLWH_UPDATE_MONGODB_ID_BY_ID % {"id": id, "mongodb_id": mongodb_id}
            cursor.execute(query_str)
            mysql_conn.commit()
        else:
            raise Exception("Cannot connect mysql")
