import logging
import os
import re
import string
import sys
from datetime import datetime, timezone
from enum import Enum
from importlib import import_module
from types import ModuleType
from typing import Any, Dict, Optional, Tuple

import pysftp  # type: ignore
from bson.decimal128 import Decimal128  # type: ignore
from crawler.constants import (
    DART_EMPTY_VALUE,
    DART_LAB_ID,
    DART_LH_SAMPLE_UUID,
    DART_RNA_ID,
    DART_ROOT_SAMPLE_ID,
    DART_STATE,
    DART_STATE_PICKABLE,
    FIELD_CH1_CQ,
    FIELD_CH1_RESULT,
    FIELD_CH1_TARGET,
    FIELD_CH2_CQ,
    FIELD_CH2_RESULT,
    FIELD_CH2_TARGET,
    FIELD_CH3_CQ,
    FIELD_CH3_RESULT,
    FIELD_CH3_TARGET,
    FIELD_CH4_CQ,
    FIELD_CH4_RESULT,
    FIELD_CH4_TARGET,
    FIELD_COORDINATE,
    FIELD_CREATED_AT,
    FIELD_DATE_TESTED,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_LAB_ID,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SOURCE,
    FIELD_UPDATED_AT,
    MLWH_CH1_CQ,
    MLWH_CH1_RESULT,
    MLWH_CH1_TARGET,
    MLWH_CH2_CQ,
    MLWH_CH2_RESULT,
    MLWH_CH2_TARGET,
    MLWH_CH3_CQ,
    MLWH_CH3_RESULT,
    MLWH_CH3_TARGET,
    MLWH_CH4_CQ,
    MLWH_CH4_RESULT,
    MLWH_CH4_TARGET,
    MLWH_COORDINATE,
    MLWH_CREATED_AT,
    MLWH_DATE_TESTED,
    MLWH_DATE_TESTED_STRING,
    MLWH_FILTERED_POSITIVE,
    MLWH_FILTERED_POSITIVE_TIMESTAMP,
    MLWH_FILTERED_POSITIVE_VERSION,
    MLWH_LAB_ID,
    MLWH_LH_SAMPLE_UUID,
    MLWH_LH_SOURCE_PLATE_UUID,
    MLWH_MONGODB_ID,
    MLWH_PLATE_BARCODE,
    MLWH_RESULT,
    MLWH_RNA_ID,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_SOURCE,
    MLWH_UPDATED_AT,
    MYSQL_DATETIME_FORMAT,
)

logger = logging.getLogger(__name__)


def current_time() -> str:
    """Generates a String containing a current timestamp in the format
    yymmdd_hhmm
    eg. 12:30 1st February 2019 becomes 190201_1230

    Returns:
        str -- A string with the current timestamp
    """
    return datetime.now().strftime("%y%m%d_%H%M")


def get_sftp_connection(config: ModuleType, username: str = None, password: str = None) -> pysftp.Connection:
    """Get a connection to the SFTP server as a context manager. The READ credentials are used by
    default but a username and password provided will override these.

    Arguments:
        config {ModuleType} -- application config

    Keyword Arguments:
        username {str} -- username to use instead of the READ username (default: {None})
        password {str} -- password for the provided username (default: {None})

    Returns:
        pysftp.Connection -- a connection to the SFTP server as a context manager
    """
    # disable host key checking:
    #   https://bitbucket.org/dundeemt/pysftp/src/master/docs/cookbook.rst#rst-header-id5
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    sftp_host = config.SFTP_HOST  # type: ignore
    sftp_port = config.SFTP_PORT  # type: ignore
    sftp_username = config.SFTP_READ_USERNAME if username is None else username  # type: ignore
    sftp_password = config.SFTP_READ_PASSWORD if username is None else password  # type: ignore

    return pysftp.Connection(
        host=sftp_host,
        port=sftp_port,
        username=sftp_username,
        password=sftp_password,
        cnopts=cnopts,
    )


def get_config(settings_module: str) -> Tuple[ModuleType, str]:
    """Get the config for the app by importing a module named by an environmental variable. This
    allows easy switching between environments and inheriting default config values.

    Arguments:
        settings_module {str} -- the settings module to load

    Returns:
        Optional[ModuleType] -- the config module loaded and available to use via `config.<param>`
    """
    try:
        if not settings_module:
            settings_module = os.environ["SETTINGS_MODULE"]

        return import_module(settings_module), settings_module  # type: ignore
    except KeyError as e:
        sys.exit(f"{e} required in environmental variables for config")


def map_mongo_to_sql_common(doc) -> Dict[str, Any]:
    """Transform common document fields into MySQL fields for MLWH.

    Arguments:
        doc {Dict[str, str]} -- Filtered information about one sample, extracted from mongodb.

    Returns:
        Dict[str, str] -- Dictionary of MySQL versions of fields
    """
    return {
        MLWH_MONGODB_ID: str(
            doc[FIELD_MONGODB_ID]
        ),  #  hexadecimal string representation of BSON ObjectId. Do ObjectId(hex_string) to turn
        # it back
        MLWH_ROOT_SAMPLE_ID: doc[FIELD_ROOT_SAMPLE_ID],
        MLWH_RNA_ID: doc[FIELD_RNA_ID],
        MLWH_PLATE_BARCODE: doc[FIELD_PLATE_BARCODE],
        MLWH_COORDINATE: unpad_coordinate(doc.get(FIELD_COORDINATE, None)),
        MLWH_RESULT: doc.get(FIELD_RESULT, None),
        MLWH_DATE_TESTED_STRING: doc.get(FIELD_DATE_TESTED, None),
        MLWH_DATE_TESTED: parse_date_tested(doc.get(FIELD_DATE_TESTED, None)),
        MLWH_SOURCE: doc.get(FIELD_SOURCE, None),
        MLWH_LAB_ID: doc.get(FIELD_LAB_ID, None),
        MLWH_CH1_TARGET: doc.get(FIELD_CH1_TARGET, None),
        MLWH_CH1_RESULT: doc.get(FIELD_CH1_RESULT, None),
        MLWH_CH1_CQ: parse_decimal128(doc.get(FIELD_CH1_CQ, None)),
        MLWH_CH2_TARGET: doc.get(FIELD_CH2_TARGET, None),
        MLWH_CH2_RESULT: doc.get(FIELD_CH2_RESULT, None),
        MLWH_CH2_CQ: parse_decimal128(doc.get(FIELD_CH2_CQ, None)),
        MLWH_CH3_TARGET: doc.get(FIELD_CH3_TARGET, None),
        MLWH_CH3_RESULT: doc.get(FIELD_CH3_RESULT, None),
        MLWH_CH3_CQ: parse_decimal128(doc.get(FIELD_CH3_CQ, None)),
        MLWH_CH4_TARGET: doc.get(FIELD_CH4_TARGET, None),
        MLWH_CH4_RESULT: doc.get(FIELD_CH4_RESULT, None),
        MLWH_CH4_CQ: parse_decimal128(doc.get(FIELD_CH4_CQ, None)),
        MLWH_FILTERED_POSITIVE: doc.get(FIELD_FILTERED_POSITIVE, None),
        MLWH_FILTERED_POSITIVE_VERSION: doc.get(FIELD_FILTERED_POSITIVE_VERSION, None),
        MLWH_FILTERED_POSITIVE_TIMESTAMP: doc.get(FIELD_FILTERED_POSITIVE_TIMESTAMP, None),
        MLWH_LH_SAMPLE_UUID: doc.get(FIELD_LH_SAMPLE_UUID, None),
        MLWH_LH_SOURCE_PLATE_UUID: doc.get(FIELD_LH_SOURCE_PLATE_UUID, None),
    }


# Strip any leading zeros from the coordinate
# eg. A01 => A1
def unpad_coordinate(coordinate):
    return re.sub(r"0(\d+)$", r"\1", coordinate) if (coordinate and isinstance(coordinate, str)) else coordinate


def map_lh_doc_to_sql_columns(doc) -> Dict[str, Any]:
    """Transform the document fields from the parsed lighthouse file into a form suitable for the MLWH.
    We are setting created_at and updated_at fields to current timestamp for inserts here,
    because it would be too slow to retrieve them from MongoDB and they would be virtually the same
    as we have only just written the mongo record.
    We also have the mongodb id, as this is after the mongo inserts and was retrieved.

     Arguments:
         doc {Dict[str, str]} -- Filtered information about one sample, extracted from csv files.

     Returns:
         Dict[str, str] -- Dictionary of MySQL versions of fields
    """
    value = map_mongo_to_sql_common(doc)
    dt = datetime.now(timezone.utc)
    value[MLWH_CREATED_AT] = dt
    value[MLWH_UPDATED_AT] = dt
    return value


def map_mongo_doc_to_sql_columns(doc) -> Dict[str, Any]:
    """Transform the document fields from the parsed mongodb samples collection.

    Arguments:
        doc {Dict[str, str]} -- Filtered information about one sample, extracted from mongodb.

    Returns:
        Dict[str, str] -- Dictionary of MySQL versions of fields
    """
    value = map_mongo_to_sql_common(doc)
    value[MLWH_CREATED_AT] = doc[FIELD_CREATED_AT]
    value[MLWH_UPDATED_AT] = doc[FIELD_UPDATED_AT]
    return value


def parse_date_tested(date_string: str) -> Any:
    """Converts date tested to MySQL format string

    Arguments:
        date_string {str} -- The date string from the document

    Returns:
        str -- The MySQL formatted string
    """
    try:
        date_time = datetime.strptime(date_string, f"{MYSQL_DATETIME_FORMAT} %Z")
        return date_time
    except Exception:
        return None


def parse_decimal128(value: Decimal128) -> Any:
    """Converts Decimal128 to MySQL compatible Decimal format

    Arguments:
        value {Decimal128} -- The number from the document or None

    Returns:
        Decimal -- converted number
    """
    try:
        dec = value.to_decimal()
        return dec
    except Exception:
        return None


def get_dart_well_index(coordinate: Optional[str]) -> Optional[int]:
    """Determines a well index from a coordinate; otherwise returns None. Well indices are
    determined by evaluating the row position, then column position. E.g. A04 -> 4, B04 -> 16.

    Arguments:
        coordinate {Optional[str]} -- The coordinate for which to determine the well index

    Returns:
        int -- the well index
    """
    if not coordinate:
        return None

    regex = r"^([A-Z])(\d{1,2})$"
    m = re.match(regex, coordinate)

    # assumes a 96-well plate with A1 - H12 wells
    if m is not None:
        col_idx = int(m.group(2))
        if 1 <= col_idx <= 12:
            multiplier = string.ascii_lowercase.index(m.group(1).lower())
            well_index = (multiplier * 12) + col_idx
            if 1 <= well_index <= 96:
                return well_index

    return None


def map_mongo_doc_to_dart_well_props(doc: Dict[str, Any]) -> Dict[str, str]:
    """Transform a mongo sample doc into DART well properties.

    Arguments:
        doc {Dict[str, str]} -- A mongo sample doc.

    Returns:
        Dict[str, str] -- Dictionary of DART property names and values.
    """
    return {
        DART_STATE: DART_STATE_PICKABLE if doc.get(FIELD_FILTERED_POSITIVE, False) else DART_EMPTY_VALUE,
        DART_ROOT_SAMPLE_ID: doc[FIELD_ROOT_SAMPLE_ID],
        DART_RNA_ID: doc[FIELD_RNA_ID],
        DART_LAB_ID: doc.get(FIELD_LAB_ID, DART_EMPTY_VALUE),
        DART_LH_SAMPLE_UUID: doc.get(FIELD_LH_SAMPLE_UUID, DART_EMPTY_VALUE),
    }
