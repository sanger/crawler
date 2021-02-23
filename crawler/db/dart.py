import logging
from typing import Dict, Optional

import pyodbc

from typing import List, Any
from crawler.types import ModifiedRow, Config

from crawler.constants import (
    DART_SET_PROP_STATUS_SUCCESS,
    DART_STATE,
    DART_STATE_NO_PLATE,
    DART_STATE_NO_PROP,
    DART_STATE_PENDING,
    FIELD_COORDINATE,
    FIELD_RESULT,
    FIELD_PLATE_BARCODE,
    FIELD_MUST_SEQUENCE,
    FIELD_ROOT_SAMPLE_ID,
    POSITIVE_RESULT_VALUE,
    FIELD_PREFERENTIALLY_SEQUENCE,
)
from crawler.exceptions import DartStateError
from crawler.helpers.general_helpers import (
    get_dart_well_index,
    map_mongo_doc_to_dart_well_props,
    is_sample_important_or_positive,
)
from crawler.sql_queries import (
    SQL_DART_ADD_PLATE,
    SQL_DART_GET_PLATE_PROPERTY,
    SQL_DART_SET_PLATE_PROPERTY,
    SQL_DART_SET_WELL_PROPERTY,
)
from crawler.types import Config, SampleDoc

logger = logging.getLogger(__name__)


def create_dart_sql_server_conn(config: Config) -> Optional[pyodbc.Connection]:
    """Create a SQL Server connection to DART with the given config parameters.

    Arguments:
        config {Config} -- application config specifying database details

    Returns:
        pyodbc.Connection -- connection object used to interact with the sql server database
    """
    dart_db_host = config.DART_DB_HOST
    dart_db_port = config.DART_DB_PORT
    dart_db_username = config.DART_DB_RW_USER
    dart_db_password = config.DART_DB_RW_PASSWORD
    dart_db_db = config.DART_DB_DBNAME
    dart_db_driver = config.DART_DB_DRIVER

    connection_string = (
        f"DRIVER={dart_db_driver};"
        f"SERVER={dart_db_host};"
        f"PORT={dart_db_port};"
        f"DATABASE={dart_db_db};"
        f"UID={dart_db_username};"
        f"PWD={dart_db_password}"
    )

    logger.debug(f"Attempting to connect to {dart_db_host} on port {dart_db_port}")

    sql_server_conn = None
    try:
        sql_server_conn = pyodbc.connect(connection_string)

        if sql_server_conn is not None:
            logger.debug("DART Connection Successful")
        else:
            logger.error("DART Connection Failed")

    except pyodbc.Error as e:
        logger.error(f"Exception on connecting to DART database: {e}")

    return sql_server_conn


def get_dart_plate_state(cursor: pyodbc.Cursor, plate_barcode: str) -> str:
    """Gets the state of a DART plate.

    Arguments:
        cursor {pyodbc.Cursor} -- The cursor with which to execute queries.
        plate_barcode {str} -- The barcode of the plate whose state to fetch.

    Returns:
        str -- The state of the plate in DART.
    """
    params = (plate_barcode, DART_STATE)

    cursor.execute(SQL_DART_GET_PLATE_PROPERTY, params)

    return str(cursor.fetchval())


def set_dart_plate_state_pending(cursor: pyodbc.Cursor, plate_barcode: str) -> bool:
    """Sets the state of a DART plate to pending.

    Arguments:
        cursor {pyodbc.Cursor} -- The cursor with which to execute queries.
        plate_barcode {str} -- The barcode of the plate whose state to set.

    Returns:
        bool -- Return True if DART was updated successfully, else False.
    """
    params = (plate_barcode, DART_STATE, DART_STATE_PENDING)
    cursor.execute(SQL_DART_SET_PLATE_PROPERTY, params)

    # assuming that the stored procedure method returns an error code, convert it to an int to make sure
    response = int(cursor.fetchval())

    return response == DART_SET_PROP_STATUS_SUCCESS


def set_dart_well_properties(
    cursor: pyodbc.Cursor, plate_barcode: str, well_props: Dict[str, str], well_index: int
) -> None:
    """Calls the DART stored procedure to add or update properties on a well

    Arguments:
        cursor {pyodbc.Cursor} -- The cursor with which to execute queries.
        plate_barcode {str} -- The barcode of the plate whose well properties to update.
        well_props {Dict[str, str]} -- The names and values of the well properties to update.
        well_index {int} -- The index of the well to update.
    """
    for prop_name, prop_value in well_props.items():
        params = (plate_barcode, prop_name, prop_value, well_index)
        # TODO: if they change state and it was picked, not perform the change
        #
        cursor.execute(SQL_DART_SET_WELL_PROPERTY, params)


def add_dart_plate_if_doesnt_exist(cursor: pyodbc.Cursor, plate_barcode: str, biomek_labclass: str) -> str:
    """Adds a plate to DART if it does not already exist. Returns the state of the plate.

    Arguments:
        cursor {pyodbc.Cursor} -- The cursor with with to execute queries.
        plate_barcode {str} -- The barcode of the plate to add.
        biomek_labclass -- The biomek labware class of the plate.

    Returns:
        str -- The state of the plate in DART.
    """
    state = get_dart_plate_state(cursor, plate_barcode)

    if state == DART_STATE_NO_PLATE:
        cursor.execute(SQL_DART_ADD_PLATE, (plate_barcode, biomek_labclass, 96))
        if set_dart_plate_state_pending(cursor, plate_barcode):
            state = DART_STATE_PENDING
        else:
            raise DartStateError(f"Unable to set the state of a DART plate {plate_barcode} to {DART_STATE_PENDING}")
    elif state == DART_STATE_NO_PROP:
        raise DartStateError(f"DART plate {plate_barcode} should have a state")

    return state


def add_dart_well_properties(
    cursor: pyodbc.Cursor, sample: SampleDoc, plate_barcode: str
) -> None:
    """Adds well properties to DART for the specified sample
        regardless of if it is important
        as fields may have been updated to not being important
        and these need to be update in Dart

    Arguments:
        cursor {pyodbc.Cursor} -- The cursor with which to execute queries.
        sample {Sample} -- The sample for which to add well properties.
        plate_barcode {str} -- The barcode of the plate to which this sample belongs.
    """
    well_index = get_dart_well_index(str(sample.get(FIELD_COORDINATE)))
    if well_index is not None:
        dart_well_props = map_mongo_doc_to_dart_well_props(sample)
        set_dart_well_properties(cursor, plate_barcode, dart_well_props, well_index)
    else:
        raise ValueError(
            f"Unable to determine DART well index for {sample[FIELD_ROOT_SAMPLE_ID]} in plate {plate_barcode}"
        )


# def add_dart_well_properties(
#     cursor: pyodbc.Cursor, sample: SampleDoc, plate_barcode: str
# ) -> None:
    """Adds well properties to DART for the specified sample if that sample is positive
        or must_sequence or preferentially_sequence

    Arguments:
        cursor {pyodbc.Cursor} -- The cursor with which to execute queries.
        sample {Sample} -- The sample for which to add well properties.
        plate_barcode {str} -- The barcode of the plate to which this sample belongs.
    """
    # if is_sample_important_or_positive(sample):
    # add_dart_well_properties(cursor, sample, plate_barcode)




def add_dart_well_properties_if_positive_or_of_importance(
    cursor: pyodbc.Cursor, sample: SampleDoc, plate_barcode: str
) -> None:
    """Adds well properties to DART for the specified sample if that sample is positive
        or must_sequence or preferentially_sequence

    Arguments:
        cursor {pyodbc.Cursor} -- The cursor with which to execute queries.
        sample {Sample} -- The sample for which to add well properties.
        plate_barcode {str} -- The barcode of the plate to which this sample belongs.
    """
    if is_sample_important_or_positive(sample):
        add_dart_well_properties(cursor, sample, plate_barcode)


        # well_index = get_dart_well_index(str(sample.get(FIELD_COORDINATE)))
        # if well_index is not None:
        #     dart_well_props = map_mongo_doc_to_dart_well_props(sample)
        #     set_dart_well_properties(cursor, plate_barcode, dart_well_props, well_index)
        # else:
        #     raise ValueError(
        #         f"Unable to determine DART well index for {sample[FIELD_ROOT_SAMPLE_ID]} in plate {plate_barcode}"
        #     )


# def _add_dart_well_properties_if_positive_old(cursor: pyodbc.Cursor, sample: SampleDoc, plate_barcode: str) -> None:
#     # if that sample is positive or must/pref seq
#     """Adds well properties to DART for the specified sample if that sample is positive.

#     Arguments:
#         cursor {pyodbc.Cursor} -- The cursor with which to execute queries.
#         sample {Sample} -- The sample for which to add well properties.
#         plate_barcode {str} -- The barcode of the plate to which this sample belongs.
#     """

#     # remove if sample field_result = positive, as sample Result may now be negative too
#     # if sample is result OR must_sequence OR preferentially_sequence

#     # if sample[FIELD_RESULT] == POSITIVE_RESULT_VALUE || must_sequence == true || preferentially_sequence == true
#     if sample[FIELD_RESULT] == POSITIVE_RESULT_VALUE:
#         well_index = get_dart_well_index(str(sample.get(FIELD_COORDINATE)))
#         if well_index is not None:
#             dart_well_props = map_mongo_doc_to_dart_well_props(sample)
#             set_dart_well_properties(cursor, plate_barcode, dart_well_props, well_index)
#         else:
#             raise ValueError(
#                 f"Unable to determine DART well index for {sample[FIELD_ROOT_SAMPLE_ID]} in plate {plate_barcode}"
#             )


# def update_priority_wells_from_docs_into_dart(samples, logging):
#     if (sql_server_connection := create_dart_sql_server_conn(self.config)) is not None:
#         try:
#             cursor = sql_server_connection.cursor()
#             # check docs_to_insert contain must_seq/ pre_seq
#             for plate_barcode, samples in groupby_transform(  # type: ignore
#                 docs_to_insert, lambda x: x[FIELD_PLATE_BARCODE]
#             ):
#                 try:
#                     plate_state = add_dart_plate_if_doesnt_exist(
#                         cursor, plate_barcode, self.centre_config["biomek_labware_class"]  # type: ignore
#                     )
#                     if plate_state == DART_STATE_PENDING:
#                         for sample in samples:
#                             add_dart_well_properties_if_positive_or_of_importance(cursor, sample, plate_barcode)  # type: ignore
#                     cursor.commit()
#                 except Exception as e:
#                     self.logging.add_error(
#                         "TYPE 22",
#                         f"DART database inserts failed for plate {plate_barcode} in file {self.file_name}",
#                     )
#                     logger.exception(e)
#                     # rollback statements executed since previous commit/rollback
#                     cursor.rollback()
#                     return False

#             logger.debug(f"DART database inserts completed successfully for file {self.file_name}")
#             return True
#         except Exception as e:
#             self.logging.add_error(
#                 "TYPE 23", f"DART database inserts failed for file {self.file_name}",
#             )
#             logger.critical(f"Critical error in file {self.file_name}: {e}")
#             logger.exception(e)
#             return False
#         finally:
#             sql_server_connection.close()
#     else:
#         self.logging.add_error(
#             "TYPE 24", f"DART database inserts failed, could not connect, for file {self.file_name}",
#         )
#         logger.critical(f"Error writing to DART for file {self.file_name}, could not create Database connection")
#         return False
