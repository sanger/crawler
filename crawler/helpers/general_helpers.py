import logging
import re
import string
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from http import HTTPStatus
from typing import Any, Dict, Iterable, List, Optional, Sequence
from mysql.connector.types import MySQLConvertibleType

import pysftp
import requests
from bson.decimal128 import Decimal128

from crawler.constants import (
    DART_EMPTY_VALUE,
    DART_LAB_ID,
    DART_LH_SAMPLE_UUID,
    DART_RNA_ID,
    DART_ROOT_SAMPLE_ID,
    DART_STATE,
    DART_STATE_PICKABLE,
    ERROR_BARACODA_COG_BARCODES,
    ERROR_BARACODA_CONNECTION,
    ERROR_BARACODA_UNKNOWN,
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
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGO_COG_UK_ID,
    FIELD_MONGO_LAB_ID,
    FIELD_MONGODB_ID,
    FIELD_MUST_SEQUENCE,
    FIELD_PLATE_BARCODE,
    FIELD_PREFERENTIALLY_SEQUENCE,
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
    MLWH_COG_UK_ID,
    MLWH_COORDINATE,
    MLWH_CREATED_AT,
    MLWH_DATE_TESTED,
    MLWH_FILTERED_POSITIVE,
    MLWH_FILTERED_POSITIVE_TIMESTAMP,
    MLWH_FILTERED_POSITIVE_VERSION,
    MLWH_IS_CURRENT,
    MLWH_LAB_ID,
    MLWH_LH_SAMPLE_UUID,
    MLWH_LH_SOURCE_PLATE_UUID,
    MLWH_MONGODB_ID,
    MLWH_MUST_SEQUENCE,
    MLWH_PLATE_BARCODE,
    MLWH_PREFERENTIALLY_SEQUENCE,
    MLWH_RESULT,
    MLWH_RNA_ID,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_SOURCE,
    MLWH_UPDATED_AT,
    RESULT_VALUE_POSITIVE,
)
from crawler.exceptions import BaracodaError
from crawler.types import Config, DartWellProp, ModifiedRowValue, SampleDoc, SourcePlateDoc

LOGGER = logging.getLogger(__name__)


def current_time() -> str:
    """Generates a String containing a current timestamp in the format yymmdd_hhmm eg. 12:30 1st February 2019 becomes
    190201_1230

    Returns:
        str -- A string with the current timestamp
    """
    return datetime.now(tz=timezone.utc).strftime("%y%m%d_%H%M")


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
    sftp_password = config.SFTP_READ_PASSWORD if not password else password

    return pysftp.Connection(
        host=sftp_host,
        port=sftp_port,
        username=sftp_username,
        password=sftp_password,
        cnopts=cnopts,
    )


def generate_baracoda_barcodes(config: Config, prefix: str, num_required: int) -> list:
    baracoda_url = f"{config.BARACODA_BASE_URL}/barcodes_group/{prefix}/new?count={num_required}"

    retries = config.BARACODA_RETRY_ATTEMPTS
    exception_msg = None
    response_json = None
    while retries > 0:
        try:
            response = requests.post(baracoda_url)
            if response.status_code == HTTPStatus.CREATED:
                response_json = response.json()
                barcodes: list = response_json["barcodes_group"]["barcodes"]
                return barcodes
            else:
                retries = retries - 1
                LOGGER.error(ERROR_BARACODA_COG_BARCODES)
                LOGGER.error(response.json())
                exception_msg = ERROR_BARACODA_COG_BARCODES
        except requests.ConnectionError as e:
            retries = retries - 1
            LOGGER.error(ERROR_BARACODA_CONNECTION)
            exception_msg = f"{ERROR_BARACODA_CONNECTION} -- {str(e)}"
        except Exception:
            retries = retries - 1
            LOGGER.error(ERROR_BARACODA_UNKNOWN)
            exception_msg = ERROR_BARACODA_UNKNOWN

    raise BaracodaError(exception_msg)


def map_mongo_to_sql_common(sample: SampleDoc) -> Dict[str, Any]:
    """Transform common mongo document fields into MySQL fields for MLWH.

    Arguments:
        doc {Sample} -- Filtered information about one sample

    Returns:
        Dict[str, Any] -- Dictionary of MySQL versions of fields
    """
    return {
        # hexadecimal string representation of BSON ObjectId. Do ObjectId(hex_string) to turn it back
        MLWH_MONGODB_ID: str(sample.get(FIELD_MONGODB_ID)),
        MLWH_ROOT_SAMPLE_ID: sample.get(FIELD_ROOT_SAMPLE_ID),
        MLWH_COG_UK_ID: sample.get(FIELD_MONGO_COG_UK_ID),
        MLWH_RNA_ID: sample.get(FIELD_RNA_ID),
        MLWH_PLATE_BARCODE: sample.get(FIELD_PLATE_BARCODE),
        MLWH_COORDINATE: unpad_coordinate(sample.get(FIELD_COORDINATE)),
        MLWH_RESULT: sample.get(FIELD_RESULT),
        MLWH_DATE_TESTED: sample.get(FIELD_DATE_TESTED),
        MLWH_SOURCE: sample.get(FIELD_SOURCE),
        MLWH_LAB_ID: sample.get(FIELD_MONGO_LAB_ID),
        # channel fields
        MLWH_CH1_TARGET: sample.get(FIELD_CH1_TARGET),
        MLWH_CH1_RESULT: sample.get(FIELD_CH1_RESULT),
        MLWH_CH1_CQ: parse_decimal128(sample.get(FIELD_CH1_CQ)),
        MLWH_CH2_TARGET: sample.get(FIELD_CH2_TARGET),
        MLWH_CH2_RESULT: sample.get(FIELD_CH2_RESULT),
        MLWH_CH2_CQ: parse_decimal128(sample.get(FIELD_CH2_CQ)),
        MLWH_CH3_TARGET: sample.get(FIELD_CH3_TARGET),
        MLWH_CH3_RESULT: sample.get(FIELD_CH3_RESULT),
        MLWH_CH3_CQ: parse_decimal128(sample.get(FIELD_CH3_CQ)),
        MLWH_CH4_TARGET: sample.get(FIELD_CH4_TARGET),
        MLWH_CH4_RESULT: sample.get(FIELD_CH4_RESULT),
        MLWH_CH4_CQ: parse_decimal128(sample.get(FIELD_CH4_CQ)),
        # filtered positive fields
        MLWH_FILTERED_POSITIVE: sample.get(FIELD_FILTERED_POSITIVE),
        MLWH_FILTERED_POSITIVE_VERSION: sample.get(FIELD_FILTERED_POSITIVE_VERSION),
        MLWH_FILTERED_POSITIVE_TIMESTAMP: sample.get(FIELD_FILTERED_POSITIVE_TIMESTAMP),
        # UUID fields
        MLWH_LH_SAMPLE_UUID: sample.get(FIELD_LH_SAMPLE_UUID),
        MLWH_LH_SOURCE_PLATE_UUID: sample.get(FIELD_LH_SOURCE_PLATE_UUID),
        # priority samples fields
        MLWH_MUST_SEQUENCE: sample.get(FIELD_MUST_SEQUENCE),
        MLWH_PREFERENTIALLY_SEQUENCE: sample.get(FIELD_PREFERENTIALLY_SEQUENCE),
    }


def unpad_coordinate(coordinate: ModifiedRowValue) -> Optional[str]:
    """Strip any leading zeros from the coordinate, eg. A01 => A1.

    Arguments:
        coordinate (str): coordinate to strip

    Returns:
        str: stripped coordinate
    """
    if not coordinate or not isinstance(coordinate, str):
        raise Exception("Cannot unpad coordinate")

    return re.sub(r"0(\d+)$", r"\1", coordinate)


def pad_coordinate(coordinate: ModifiedRowValue) -> str:
    """Add leading zeros to the coordinate, eg. A1 => A01.

    Arguments:
        coordinate (str): coordinate to pad

    Returns:
        str: padded coordinate with 2 characters adding 0's
    """
    if not coordinate or not isinstance(coordinate, str):
        raise Exception("Expecting string coordinate to pad")

    return f"{coordinate[0]}{coordinate[1:].zfill(2)}"


def map_mongo_sample_to_mysql(doc: SampleDoc, copy_date: bool = False) -> Dict[str, Any]:
    """Transform the sample document fields into a form suitable for the MLWH. We are setting created_at and updated_at
    fields to current timestamp for inserts here, because it would be too slow to retrieve them from mongo and they
    would be virtually the same as we have only just written the mongo record. We also have the mongodb '_id', as this
    is after the mongo inserts.

     Arguments:
         doc {Sample} -- Filtered information about one sample

     Returns:
         Dict[str, Any] -- Dictionary of MySQL versions of fields
    """
    value = map_mongo_to_sql_common(doc)

    if copy_date:
        value[MLWH_CREATED_AT] = doc[FIELD_CREATED_AT]
        value[MLWH_UPDATED_AT] = doc[FIELD_UPDATED_AT]
    else:
        dt = datetime.now(tz=timezone.utc)

        value[MLWH_CREATED_AT] = dt
        value[MLWH_UPDATED_AT] = dt

    return value


def set_is_current_on_mysql_samples(samples: Iterable[Dict[str, str]]) -> Sequence[Dict[str, MySQLConvertibleType]]:
    """Creates a copy of the samples passed in, adding is_current values to each sample.
    is_current will be True for all samples unless there is a repeated RNA ID, in which case
    only the last one is set to True.

    Arguments:
        samples: Iterable[Dict[str, str]] -- An iterable containing dictionaries of sample data.

    Returns:
        A list containing new copies of the samples in the same order with is_current populated for each.
    """
    reversed_samples: List[Dict[str, Any]] = []
    existing_rna_ids = set([""])

    # Process samples in reverse order so that duplicate RNA IDs presented earlier in the file are processed later in
    # the loop and can be set to is_current = False.  We will reverse the parsed samples again when returning to retain
    # the original order.
    for sample in reversed(list(samples)):
        try:
            rna_id = sample[MLWH_RNA_ID]
            reversed_samples.append({**sample, MLWH_IS_CURRENT: rna_id not in existing_rna_ids})
            existing_rna_ids.add(rna_id)
        except KeyError:
            # If there is no RNA ID (shouldn't happen) set is_current to False
            reversed_samples.append({**sample, MLWH_IS_CURRENT: False})

    return list(reversed(reversed_samples))


def parse_decimal128(value: ModifiedRowValue) -> Optional[Decimal]:
    """Converts mongo Decimal128 to MySQL compatible Decimal format.

    Arguments:
        value {Decimal128} -- The number from the document or None

    Returns:
        Decimal -- converted number
    """
    if not isinstance(value, Decimal128):
        return None

    return (Decimal)(value.to_decimal())


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


def is_sample_positive(sample: SampleDoc) -> bool:
    return sample.get(FIELD_RESULT, False) == RESULT_VALUE_POSITIVE


def is_sample_pickable(sample: SampleDoc) -> bool:
    return (sample.get(FIELD_FILTERED_POSITIVE, False) is True) or (sample.get(FIELD_MUST_SEQUENCE, False) is True)


def map_mongo_doc_to_dart_well_props(sample: SampleDoc) -> DartWellProp:
    """Transform a mongo sample doc into DART well properties.

    Arguments:
        doc {Sample} -- A mongo sample doc.

    Returns:
        DartWellProp -- Dictionary of DART property names and values.
    """

    return {
        DART_STATE: DART_STATE_PICKABLE if is_sample_pickable(sample) else DART_EMPTY_VALUE,
        DART_ROOT_SAMPLE_ID: str(sample[FIELD_ROOT_SAMPLE_ID]),
        DART_RNA_ID: str(sample[FIELD_RNA_ID]),
        DART_LAB_ID: str(sample.get(FIELD_MONGO_LAB_ID, DART_EMPTY_VALUE)),
        DART_LH_SAMPLE_UUID: str(sample.get(FIELD_LH_SAMPLE_UUID, DART_EMPTY_VALUE)),
    }


def create_source_plate_doc(plate_barcode: str, lab_id: str) -> SourcePlateDoc:
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
        FIELD_MONGO_LAB_ID: lab_id,
        FIELD_UPDATED_AT: datetime.now(tz=timezone.utc),
        FIELD_CREATED_AT: datetime.now(tz=timezone.utc),
    }


def extract_duplicated_values(values):
    """A helper method for finding duplicated values in an iterable.
    The returned set will contain only values that existed more than once.

    Arguments:
        values: Iterable -- An iterable containing hashable values.

    Returns:
        Set of values that were duplicated at least once.
    """
    seen = set()
    dupes = set()

    for x in values:
        if x in seen:
            dupes.add(x)
        else:
            seen.add(x)

    return dupes


def is_found_in_list(needle: str, haystack: List[str]) -> bool:
    """A helper method for finding a string contained in any one of a list of strings.

    Arguments:
        needle: str -- The string to identify in the list of strings.
        haystack: List[str] -- A list of strings that might contain the needle.

    Returns:
        True if the needle exists as a sub-string of any of the strings in the haystack.
        False if the needle cannot be found in any string in the haystack.
    """
    return any([needle in bail for bail in haystack])
