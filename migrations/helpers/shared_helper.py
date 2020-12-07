import sys
import traceback
from datetime import datetime

from crawler.constants import MONGO_DATETIME_FORMAT


def print_exception() -> None:
    print(f"An exception occurred, at {datetime.now()}")
    e = sys.exc_info()
    print(e[0])  # exception type
    print(e[1])  # exception message
    if e[2]:  # traceback
        traceback.print_tb(e[2], limit=10)


def valid_datetime_string(s_datetime: str) -> bool:
    try:
        dt = datetime.strptime(s_datetime, MONGO_DATETIME_FORMAT)
        if dt is None:
            return False
        return True
    except Exception:
        print_exception()
        return False
