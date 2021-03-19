import logging
import sys
import traceback
from datetime import datetime
from typing import Optional

from crawler.constants import MONGO_DATETIME_FORMAT

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
