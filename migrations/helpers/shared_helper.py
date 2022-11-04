import logging
import os
import stat
import sys
import traceback
from csv import DictReader
from datetime import datetime
from typing import List, Optional

from crawler.constants import FIELD_MONGO_SOURCE_PLATE_BARCODE, MONGO_DATETIME_FORMAT
from crawler.types import Config

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


def extract_barcodes(config: Config, filepath: str) -> List[str]:
    """Extract the list of barcodes from the csv file

    Arguments:
        config {Config} -- application config specifying database details
        filepath {str} -- the filepath of the csv file containing the list of source plate barcodes

    Returns:
        List[str] -- list of source plate barcodes
    """
    extracted_barcodes: List[str] = []
    try:
        with open(filepath, newline="") as csvfile:
            csvreader = DictReader(csvfile)
            for row in csvreader:
                extracted_barcodes.append(row[FIELD_MONGO_SOURCE_PLATE_BARCODE])

    except Exception as e:
        logger.critical("Error reading source barcodes file " f"{filepath}")
        logger.exception(e)

    return extracted_barcodes


def validate_args(config: Config, s_filepath: str) -> str:
    """Validate the supplied arguments

    Arguments:
        config {Config} -- application config specifying database details
        s_filepath {str} -- the filepath of the csv file containing the list of source plate barcodes

    Returns:
        str -- the filepath if valid
    """
    base_msg = "Aborting run: "
    if not config:
        msg = f"{base_msg} Config required"
        logger.error(msg)
        raise Exception(msg)

    if not valid_filepath(s_filepath):
        msg = f"{base_msg} Unable to confirm valid csv file from supplied filepath"
        logger.error(msg)
        raise Exception(msg)

    filepath = s_filepath

    return filepath


def valid_filepath(s_filepath: str) -> bool:
    """Determine if the filepath argument supplied corresponds to a csv file

    Arguments:
        s_filepath {str} -- the filepath of the csv file containing the list of source plate barcodes

    Returns:
        bool -- whether the filepath corresponds to a csv file
    """
    if stat.S_ISREG(os.lstat(s_filepath).st_mode):
        file_name, file_extension = os.path.splitext(s_filepath)
        return file_extension == ".csv"

    return False
