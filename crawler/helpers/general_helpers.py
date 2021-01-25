import logging
import os
import re
import string
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from importlib import import_module
from typing import Any, Dict, Optional, Tuple, cast

import pysftp
from bson.decimal128 import Decimal128

from crawler.constants import (
    DART_EMPTY_VALUE,
    DART_LAB_ID,
    DART_LH_SAMPLE_UUID,
    DART_RNA_ID,
    DART_ROOT_SAMPLE_ID,
    DART_STATE,
    DART_STATE_PICKABLE,
    FIELD_BARCODE,
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
from crawler.types import Config, DartWellProp, Sample, SourcePlate

logger = logging.getLogger(__name__)


def current_time() -> str:
    """Generates a String containing a current timestamp in the format yymmdd_hhmm eg. 12:30 1st February 2019 becomes
    190201_1230

    Returns:
        str -- A string with the current timestamp
    """
    return datetime.now().strftime("%y%m%d_%H%M")


def get_sftp_connection(config: Config, username: str = "", password: str = "") -> pysftp.Connection:
    """Get a connection to the SFTP server as a context manager. The READ credentials are used by default but a username
    and password provided will override these.

    Arguments:
        config (Config): application config module
        username (str, optional): username to use instead of the READ username. Defaults to "".
        password (str, optional): password for the provided username. Defaults to "".

    Returns:
        pysftp.Connection: a connection to the SFTP server as a context manager
    """
    # disable host key checking:
    #   https://bitbucket.org/dundeemt/pysftp/src/master/docs/cookbook.rst#rst-header-id5
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    sftp_host = config.SFTP_HOST
    sftp_port = config.SFTP_PORT
    sftp_username = config.SFTP_READ_USERNAME if not username else username
    sftp_password = config.SFTP_READ_PASSWORD if not username else password

    return pysftp.Connection(
        host=sftp_host,
        port=sftp_port,
        username=sftp_username,
        password=sftp_password,
        cnopts=cnopts,
    )


def get_config(settings_module: str = "") -> Tuple[Config, str]:
    """Get the config for the app by importing a module named by an environmental variable. This allows easy switching
    between environments and inheriting default config values.

    Arguments:
        settings_module (str, optional): the settings module to load. Defaults to "".

    Returns:
        Tuple[Config, str]: tuple with the config module loaded and available to use via `config.<param>` and the
        settings module used
    """
    try:
        if not settings_module:
            settings_module = os.environ["SETTINGS_MODULE"]

        config_module = cast(Config, import_module(settings_module))

        return config_module, settings_module
    except KeyError as e:
        sys.exit(f"{e} required in environmental variables for config")


def map_mongo_to_sql_common(sample: Sample) -> Dict[str, Any]:
    """Transform common mongo document fields into MySQL fields for MLWH.

    Arguments:
        doc {Sample} -- Filtered information about one sample, extracted from mongodb.

    Returns:
        Dict[str, Any] -- Dictionary of MySQL versions of fields
    """
    return {
        # hexadecimal string representation of BSON ObjectId. Do ObjectId(hex_string) to turn it back
        MLWH_MONGODB_ID: str(sample[FIELD_MONGODB_ID]),
        MLWH_ROOT_SAMPLE_ID: sample[FIELD_ROOT_SAMPLE_ID],
        MLWH_RNA_ID: sample[FIELD_RNA_ID],
        MLWH_PLATE_BARCODE: sample[FIELD_PLATE_BARCODE],
        MLWH_COORDINATE: unpad_coordinate(sample.get(FIELD_COORDINATE, None)),
        MLWH_RESULT: sample.get(FIELD_RESULT, None),
        MLWH_DATE_TESTED_STRING: sample.get(FIELD_DATE_TESTED, None),
        MLWH_DATE_TESTED: parse_date_tested(sample.get(FIELD_DATE_TESTED, None)),
        MLWH_SOURCE: sample.get(FIELD_SOURCE, None),
        MLWH_LAB_ID: sample.get(FIELD_LAB_ID, None),
        # channel fields
        MLWH_CH1_TARGET: sample.get(FIELD_CH1_TARGET, None),
        MLWH_CH1_RESULT: sample.get(FIELD_CH1_RESULT, None),
        MLWH_CH1_CQ: parse_decimal128(sample.get(FIELD_CH1_CQ, None)),
        MLWH_CH2_TARGET: sample.get(FIELD_CH2_TARGET, None),
        MLWH_CH2_RESULT: sample.get(FIELD_CH2_RESULT, None),
        MLWH_CH2_CQ: parse_decimal128(sample.get(FIELD_CH2_CQ, None)),
        MLWH_CH3_TARGET: sample.get(FIELD_CH3_TARGET, None),
        MLWH_CH3_RESULT: sample.get(FIELD_CH3_RESULT, None),
        MLWH_CH3_CQ: parse_decimal128(sample.get(FIELD_CH3_CQ, None)),
        MLWH_CH4_TARGET: sample.get(FIELD_CH4_TARGET, None),
        MLWH_CH4_RESULT: sample.get(FIELD_CH4_RESULT, None),
        MLWH_CH4_CQ: parse_decimal128(sample.get(FIELD_CH4_CQ, None)),
        # filtered positive fields
        MLWH_FILTERED_POSITIVE: sample.get(FIELD_FILTERED_POSITIVE, None),
        MLWH_FILTERED_POSITIVE_VERSION: sample.get(FIELD_FILTERED_POSITIVE_VERSION, None),
        MLWH_FILTERED_POSITIVE_TIMESTAMP: sample.get(FIELD_FILTERED_POSITIVE_TIMESTAMP, None),
        # UUID fields
        MLWH_LH_SAMPLE_UUID: sample.get(FIELD_LH_SAMPLE_UUID, None),
        MLWH_LH_SOURCE_PLATE_UUID: sample.get(FIELD_LH_SOURCE_PLATE_UUID, None),
    }


def unpad_coordinate(coordinate: str) -> str:
    """Strip any leading zeros from the coordinate, eg. A01 => A1.

    Arguments:
        coordinate (str): coordinate to strip

    Returns:
        str: stripped coordinate
    """
    return re.sub(r"0(\d+)$", r"\1", coordinate) if (coordinate and isinstance(coordinate, str)) else coordinate


def map_lh_doc_to_sql_columns(doc: Dict[str, str]) -> Dict[str, Any]:
    """Transform the document fields from the parsed lighthouse file into a form suitable for the MLWH.
    We are setting created_at and updated_at fields to current timestamp for inserts here,
    because it would be too slow to retrieve them from MongoDB and they would be virtually the same
    as we have only just written the mongo record.
    We also have the mongodb id, as this is after the mongo inserts and was retrieved.

     Arguments:
         doc {Dict[str, str]} -- Filtered information about one sample, extracted from csv files.

     Returns:
         Dict[str, Any] -- Dictionary of MySQL versions of fields
    """
    value = map_mongo_to_sql_common(doc)
    dt = datetime.now(timezone.utc)
    value[MLWH_CREATED_AT] = dt
    value[MLWH_UPDATED_AT] = dt
    return value


def map_mongo_doc_to_sql_columns(doc: Sample) -> Dict[str, Any]:
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


def parse_date_tested(date_string: str) -> Optional[datetime]:
    """Converts date tested to MySQL format datetime

    Arguments:
        date_string {str} -- The date string from the document

    Returns:
        datetime -- The MySQL formatted datetime
    """
    try:
        return datetime.strptime(date_string, f"{MYSQL_DATETIME_FORMAT} %Z")
    except Exception:
        return None


def parse_decimal128(value: Decimal128) -> Optional[Decimal]:
    """Converts mongo Decimal128 to MySQL compatible Decimal format.

    Arguments:
        value {Decimal128} -- The number from the document or None

    Returns:
        Decimal -- converted number
    """
    try:
        return cast(Decimal, value.to_decimal())
    except Exception:
        return None


def get_dart_well_index(coordinate: Optional[str]) -> Optional[int]:
    """Determines a well index from a coordinate; otherwise returns None. Well indices are determined by evaluating the
    row position, then column position. E.g. A04 -> 4, B04 -> 16.

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


def map_mongo_doc_to_dart_well_props(sample: Sample) -> DartWellProp:
    """Transform a mongo sample doc into DART well properties.

    Arguments:
        doc {Sample} -- A mongo sample doc.

    Returns:
        DartWellProp -- Dictionary of DART property names and values.
    """
    return {
        DART_STATE: DART_STATE_PICKABLE if sample.get(FIELD_FILTERED_POSITIVE, False) else DART_EMPTY_VALUE,
        DART_ROOT_SAMPLE_ID: sample[FIELD_ROOT_SAMPLE_ID],
        DART_RNA_ID: sample[FIELD_RNA_ID],
        DART_LAB_ID: sample.get(FIELD_LAB_ID, DART_EMPTY_VALUE),
        DART_LH_SAMPLE_UUID: sample.get(FIELD_LH_SAMPLE_UUID, DART_EMPTY_VALUE),
    }


def create_source_plate_doc(plate_barcode: str, lab_id: str) -> SourcePlate:
    """Creates a new source plate document to be inserted into mongo.

    Arguments:
        plate_barcode {str} -- The plate barcode to assign to the new source plate.
        lab_id {str} -- The lab ID to assign to the new source plate.

    Returns:
        SourcePlate -- The new mongo source plate doc.
    """
    return {
        FIELD_LH_SOURCE_PLATE_UUID: str(uuid.uuid4()),
        FIELD_BARCODE: plate_barcode,
        FIELD_LAB_ID: lab_id,
        FIELD_UPDATED_AT: datetime.now(),
        FIELD_CREATED_AT: datetime.now(),
    }
