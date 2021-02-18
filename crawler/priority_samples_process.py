#
# This helper module will contain all functions required for running Step 2
#
import logging
import logging.config

from typing import Any, List,  Dict
from crawler.types import ModifiedRow
from crawler.db.mongo import (
    get_mongo_collection,
)

from crawler.constants import (
    COLLECTION_PRIORITY_SAMPLES,
    COLLECTION_SAMPLES,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_MUST_SEQUENCE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    FIELD_PLATE_BARCODE,
    DART_STATE_PENDING
)

from crawler.helpers.general_helpers import (
    map_mongo_sample_to_mysql,
)

from crawler.helpers.logging_helpers import LoggingCollection

from crawler.db.mysql import (
    create_mysql_connection,
    run_mysql_executemany_query
)

from crawler.db.dart import (
    create_dart_sql_server_conn,
    add_dart_plate_if_doesnt_exist,
    add_dart_well_properties_if_positive_or_of_importance
)

from crawler.sql_queries import SQL_MLWH_MULTIPLE_INSERT

logger = logging.getLogger(__name__)

# posisble move to seperate script
def step_two(db) -> None:
    """
    Description
    Arguments:
        x {Type} -- description
    """
    import pdb
    pdb.set_trace()

    logger.info(f"Starting Step 2")
    logging_collection = LoggingCollection()

    all_unprocessed_priority_samples = get_all_unprocessed_priority_samples(db)
    unprocessed_root_sample_ids = list(map(lambda x: x[FIELD_ROOT_SAMPLE_ID], all_unprocessed_priority_samples))

    samples = get_samples_for_root_sample_ids(db, unprocessed_root_sample_ids)

    merge_priority_samples_into_docs_to_insert(all_unprocessed_priority_samples, samples)

    # Create all samples in MLWH with docs_to_insert including must_seq/ pre_seq
    # TODO remove file name?
    file_name_substitute = "STEP_TWO"
    mlwh_success = update_priority_samples_into_mlwh(samples, file_name_substitute)

    # add to the DART database if the config flag is set and we have successfully updated the MLWH
    if mlwh_success:
        logger.info("MLWH insert successful and adding to DART")

        #  Create in DART with docs_to_insert including must_seq/ pre_seq
        #  use docs_to_insert to update DART
        dart_success = insert_plates_and_wells_from_docs_into_dart_for_priority_samples(samples)
        if dart_success:
            # use stored identifiers to update priority_samples table to processed true
            all_unprocessed_priority_samples_root_samples_id = list(map(lambda x: x[FIELD_ROOT_SAMPLE_ID], all_unprocessed_priority_samples))
            update_unprocessed_priority_samples_to_processed(db, all_unprocessed_priority_samples_root_samples_id)


# TODO: refactor duplicated function
def insert_plates_and_wells_from_docs_into_dart_for_priority_samples(self, docs_to_insert: List[ModifiedRow]) -> bool:
    """Insert plates and wells into the DART database.
    Create in DART with docs_to_insert including must_seq/ pre_seq
    use docs_to_insert to update DART

    Arguments:
        docs_to_insert {List[ModifiedRow]} -- List of filtered sample information extracted from CSV files.

    Returns:
        TODO: check return False
        {bool} -- True if the insert was successful; otherwise False
    """
    if (sql_server_connection := create_dart_sql_server_conn(self.config)) is not None:
        try:
            cursor = sql_server_connection.cursor()
            # check docs_to_insert contain must_seq/ pre_seq
            for plate_barcode, samples in groupby_transform(  # type: ignore
                docs_to_insert, lambda x: x[FIELD_PLATE_BARCODE]
            ):
                try:
                    plate_state = add_dart_plate_if_doesnt_exist(
                        cursor, plate_barcode, self.centre_config["biomek_labware_class"]  # type: ignore
                    )
                    if plate_state == DART_STATE_PENDING:
                        for sample in samples:
                            add_dart_well_properties_if_positive_or_of_importance(cursor, sample, plate_barcode)  # type: ignore
                    cursor.commit()
                except Exception as e:
                    self.logging.add_error(
                        "TYPE 22",
                        f"DART database inserts failed for plate {plate_barcode} in file {self.file_name}",
                    )
                    logger.exception(e)
                    # rollback statements executed since previous commit/rollback
                    cursor.rollback()
                    return False

            logger.debug(f"DART database inserts completed successfully for file {self.file_name}")
            return True
        except Exception as e:
            self.logging.add_error(
                "TYPE 23", f"DART database inserts failed for file {self.file_name}",
            )
            logger.critical(f"Critical error in file {self.file_name}: {e}")
            logger.exception(e)
            return False
        finally:
            sql_server_connection.close()
    else:
        self.logging.add_error(
            "TYPE 24", f"DART database inserts failed, could not connect, for file {self.file_name}",
        )
        logger.critical(f"Error writing to DART for file {self.file_name}, could not create Database connection")
        return False

# TODO: refactor duplicated function
def update_priority_samples_into_mlwh(self, samples: List[Any], file_name) -> bool:
    """Insert sample records into the MLWH database from the parsed file information, including the corresponding
    mongodb _id
    Create all samples in MLWH with samples including must_seq/ pre_seq

    Arguments:
        samples {List[ModifiedRow]} -- List of filtered sample information extracted from CSV files.

    Returns:
        {bool} -- True if the insert was successful; otherwise False
    """
    values: List[Dict[str, Any]] = []

    for sample_doc in samples:
        values.append(map_mongo_sample_to_mysql(sample_doc))

    mysql_conn = create_mysql_connection(self.config, False)

    if mysql_conn is not None and mysql_conn.is_connected():
        try:
            run_mysql_executemany_query(mysql_conn, SQL_MLWH_MULTIPLE_INSERT, values)

            logger.debug(f"MLWH database inserts completed successfully for file {file_name}")
            return True
        except Exception as e:
            self.logging_collection.add_error(
                "TYPE 28", f"MLWH database inserts failed for priority samples",
            )
            logger.critical(f"Critical error while processing priority samples': {e}")
            logger.exception(e)
    else:
        self.logging_collection.add_error(
            "TYPE 29", f"MLWH database inserts failed, could not connect",
        )
        logger.critical(f"Error writing to MLWH for file {file_name}, could not create Database connection")

    return False


def merge_priority_samples_into_docs_to_insert(priority_samples: List[Any], docs_to_insert) -> None:
    """
    Updates the sample records with must_sequence and preferentially_sequence values

    for each successful add sample, merge into docs_to_insert_mlwh
    with must_sequence and preferentially_sequence values

    Arguments:
        priority_samples  - priority samples to update docs_to_insert with
        docs_to_insert {List[ModifiedRow]} -- the sample records to update
    """
    priority_root_sample_ids = list(map(lambda x: x[FIELD_ROOT_SAMPLE_ID], priority_samples))

    for doc in docs_to_insert:
        root_sample_id = doc[FIELD_ROOT_SAMPLE_ID]
        if root_sample_id in priority_root_sample_ids:
            priority_sample = list(filter(lambda x: x[FIELD_ROOT_SAMPLE_ID] == root_sample_id, priority_samples))[0]
            doc[FIELD_MUST_SEQUENCE] = priority_sample[FIELD_MUST_SEQUENCE]
            doc[FIELD_PREFERENTIALLY_SEQUENCE] = priority_sample[FIELD_PREFERENTIALLY_SEQUENCE]


def get_samples_for_root_sample_ids(db, root_sample_ids) -> List[Any]:
    """
    Description
    Arguments:
        x {Type} -- description
    """
    samples_collection = get_mongo_collection(db, COLLECTION_SAMPLES)
    return list(map(lambda x: x, samples_collection.find({FIELD_ROOT_SAMPLE_ID: {"$in": root_sample_ids}})))

# TODO: refactor duplicated function
def update_unprocessed_priority_samples_to_processed(db, root_sample_ids) -> bool:
    """
    Description
    use stored identifiers to update priority_samples table to processed true
    Arguments:
        x {Type} -- description
    """
    priority_samples_collection = get_mongo_collection(db, COLLECTION_PRIORITY_SAMPLES)
    for root_sample_id in root_sample_ids:
        priority_samples_collection.update({"Root Sample ID": root_sample_id}, {"$set": {"processed": True}})
    logger.info("Mongo update of processed for priority samples successful")


def get_all_unprocessed_priority_samples(db) -> List[Any]:
    """
    Description
    Arguments:
        x {Type} -- description
    """
    unprocessed = { "processed": False }
    query = unprocessed

    priority_samples_collection = get_mongo_collection(db, COLLECTION_PRIORITY_SAMPLES)
    priority_sample_cursor = priority_samples_collection.find(query)
    return list(priority_sample_cursor)



