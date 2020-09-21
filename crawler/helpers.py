import logging
import os
import sys

from datetime import (
    datetime,
    timezone,
)
from importlib import import_module
from types import ModuleType
from enum import Enum
from typing import Dict, Any, Tuple

import pysftp  # type: ignore

from crawler.constants import (
    FIELD_MONGODB_ID,
    FIELD_COORDINATE,
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_LINE_NUMBER,
    FIELD_FILE_NAME,
    FIELD_FILE_NAME_DATE,
    FIELD_CREATED_AT,
    FIELD_UPDATED_AT,
    FIELD_SOURCE,
    MLWH_MONGODB_ID,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_RNA_ID,
    MLWH_PLATE_BARCODE,
    MLWH_COORDINATE,
    MLWH_RESULT,
    MLWH_DATE_TESTED_STRING,
    MLWH_DATE_TESTED,
    MLWH_SOURCE,
    MLWH_LAB_ID,
    MLWH_CREATED_AT,
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


def get_sftp_connection(
    config: ModuleType, username: str = None, password: str = None
) -> pysftp.Connection:
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


    if doc.get(FIELD_DATE_TESTED, None) is not None:
        date_tested = parse_date_tested(doc[FIELD_DATE_TESTED])
    else:
        date_tested = None

    return {
        MLWH_MONGODB_ID: str(doc[FIELD_MONGODB_ID]), # hexadecimal string representation of BSON ObjectId. Do ObjectId(hex_string) to turn it back
        MLWH_ROOT_SAMPLE_ID: doc[FIELD_ROOT_SAMPLE_ID],
        MLWH_RNA_ID: doc[FIELD_RNA_ID],
        MLWH_PLATE_BARCODE: doc[FIELD_PLATE_BARCODE],
        MLWH_COORDINATE: doc.get(FIELD_COORDINATE, None),
        MLWH_RESULT: doc.get(FIELD_RESULT, None),
        MLWH_DATE_TESTED: date_tested,
        MLWH_DATE_TESTED_STRING: doc.get(FIELD_DATE_TESTED, None),
        MLWH_SOURCE: doc.get(FIELD_SOURCE, None),
        MLWH_LAB_ID: doc.get(FIELD_LAB_ID, None),
    }

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
    dt = datetime.strftime(datetime.now(timezone.utc), MYSQL_DATETIME_FORMAT)
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
    value[MLWH_CREATED_AT] = datetime.strftime(doc[FIELD_CREATED_AT], MYSQL_DATETIME_FORMAT)
    value[MLWH_UPDATED_AT] = datetime.strftime(doc[FIELD_UPDATED_AT], MYSQL_DATETIME_FORMAT)
    return value

def parse_date_tested(date_string: str) -> str:
    """Converts date tested to MySQL format string

        Arguments:
            date_string {str} -- The date string from the document

        Returns:
            str -- The MySQL formatted string
    """
    try:
        date_time = datetime.strptime(date_string, f"{MYSQL_DATETIME_FORMAT} %Z")
    except:
        return ''

    if date_string.find('UTC') != -1:
        # timezone doesn't get set despite the '%Z' in the format string, unless we do this
        date_time = date_time.replace(tzinfo=timezone.utc)

    date_time_str = datetime.strftime(date_time, MYSQL_DATETIME_FORMAT)

    return date_time_str

class ErrorLevel(Enum):
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5
    FATAL = 6


class AggregateTypeBase:
    """ Base class for Aggregate types. Should not be instantiated directly.
    """

    def __init__(self):
        self.error_level = ErrorLevel.DEBUG
        self.count_errors = 0
        self.max_errors = -1
        self.message = ""
        self.short_display_description = ""
        self.type_str = ""

    def add_error(self, message) -> None:
        """Adds a new error to the aggregate type. Checks max_errors to decide whether message should be appended
            to the default message or not. Increments total counter for this type of error.

            Arguments:
                message {str} -- the specific message for this error e.g. with a line number or barcode
        """
        self.count_errors += 1
        if self.max_errors > 0 and self.count_errors <= self.max_errors:
            self.message = self.message + f" (e.g. {message})"

    def get_message(self):
        return self.message

    def get_report_message(self):
        return f"Total number of {self.short_display_description} errors ({self.type_str}): {self.count_errors}"


# See confluence for full table of aggregate types https://ssg-confluence.internal.sanger.ac.uk/display/PSDPUB/i.+Low+Occupancy+Cherry+Picking


class AggregateType1(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 1"
        self.error_level = ErrorLevel.DEBUG
        self.message = f"DEBUG: Blank rows in files. ({self.type_str})"
        self.short_display_description = "Blank row"


class AggregateType2(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 2"
        self.error_level = ErrorLevel.CRITICAL
        self.message = f"CRITICAL: Files where we do not have the expected main column headers of Root Sample ID, RNA ID and Result. ({self.type_str})"
        self.short_display_description = "Missing header column"


class AggregateType3(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 3"
        self.error_level = ErrorLevel.WARNING
        self.message = f"WARNING: Sample rows that have Root Sample ID value but no other information. ({self.type_str})"
        self.max_errors = 5
        self.short_display_description = "Only root sample id"


class AggregateType4(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 4"
        self.error_level = ErrorLevel.ERROR
        self.message = f"ERROR: Sample rows that have Root Sample ID and Result values but no RNA ID (no plate barcode). ({self.type_str})"
        self.max_errors = 5
        self.short_display_description = "No plate barcode"


class AggregateType5(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 5"
        self.error_level = ErrorLevel.WARNING
        self.message = f"WARNING: Duplicates detected within the file. ({self.type_str})"
        self.max_errors = 5
        self.short_display_description = "Duplicates within file"


class AggregateType6(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 6"
        self.error_level = ErrorLevel.WARNING
        self.message = (
            f"WARNING: Duplicates detected matching rows in previous files. ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "Duplicates to previous files"


class AggregateType7(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 7"
        self.error_level = ErrorLevel.WARNING
        self.message = f"WARNING: Samples rows matching previously uploaded rows but with different test date. ({self.type_str})"
        self.max_errors = 5
        self.short_display_description = "Different test date"


# Type 8 is valid and not logged (re-tests of samples)


class AggregateType9(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 9"
        self.error_level = ErrorLevel.CRITICAL
        self.message = f"CRITICAL: Sample rows failing to match expected format (regex) for RNA ID field. ({self.type_str})"
        self.max_errors = 5
        self.short_display_description = "Failed regex on plate barcode"


class AggregateType10(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 10"
        self.error_level = ErrorLevel.CRITICAL
        self.message = (
            f"CRITICAL: File is unexpected type and cannot be processed. ({self.type_str})"
        )
        self.max_errors = -1
        self.short_display_description = "File wrong type"


# Type 11 is blacklisted file, not logged


class AggregateType12(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 12"
        self.error_level = ErrorLevel.ERROR
        self.message = (
            f"ERROR: Sample rows that do not contain a Lab ID. ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "No Lab ID"

class AggregateType13(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 13"
        self.error_level = ErrorLevel.WARNING
        self.message = (
            f"ERROR: Sample rows that contain unexpected columns. ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "Extra column(s)"

class AggregateType14(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 14"
        self.error_level = ErrorLevel.CRITICAL
        self.message = f"CRITICAL: Files where the MLWH database insert has failed. ({self.type_str})"
        self.short_display_description = "Failed MLWH inserts"

class AggregateType15(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 15"
        self.error_level = ErrorLevel.CRITICAL
        self.message = f"CRITICAL: Files where the MLWH database connection could not be made. ({self.type_str})"
        self.short_display_description = "Failed MLWH connection"


class LoggingCollection:
    def __init__(self):
        self.aggregator_types = {
            "TYPE 1": AggregateType1(),
            "TYPE 2": AggregateType2(),
            "TYPE 3": AggregateType3(),
            "TYPE 4": AggregateType4(),
            "TYPE 5": AggregateType5(),
            "TYPE 6": AggregateType6(),
            "TYPE 7": AggregateType7(),
            "TYPE 9": AggregateType9(),
            "TYPE 10": AggregateType10(),
            "TYPE 12": AggregateType12(),
            "TYPE 13": AggregateType13(),
            "TYPE 14": AggregateType14(),
            "TYPE 15": AggregateType15(),
        }

    def add_error(self, aggregate_error_type, message):
        self.aggregator_types[aggregate_error_type].add_error(message)

    def get_aggregate_messages(self):
        msgs = []
        for (k, v) in sorted(self.aggregator_types.items()):
            if v.count_errors > 0:
                msgs.append(v.get_message())

        return msgs

    def get_aggregate_total_messages(self):
        msgs = []
        for (k, v) in sorted(self.aggregator_types.items()):
            if v.count_errors > 0:
                msgs.append(v.get_report_message())

        return msgs

    def get_messages_for_import(self):
        return self.get_aggregate_total_messages() + self.get_aggregate_messages()

    def get_count_of_all_errors_and_criticals(self):
        count = 0
        for (k, v) in self.aggregator_types.items():
            if v.error_level == ErrorLevel.ERROR or v.error_level == ErrorLevel.CRITICAL:
                count += v.count_errors

        return count
