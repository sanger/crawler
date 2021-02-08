"""
    Split test_file_processing.py into numerous files until a larger refactor can test classes more succinctly.
"""
from datetime import datetime, timezone

from crawler.constants import FIELD_DATE_TESTED
from crawler.file_processing import CentreFile
from crawler.types import ModifiedRow


def test_is_valid_date_format(centre_file: CentreFile) -> None:
    assert centre_file.is_valid_date_format({}, 1, FIELD_DATE_TESTED) == (True, {})

    date_time = datetime(year=2020, month=11, day=22, hour=4, minute=36, second=38, tzinfo=timezone.utc)

    # "Normal" datetime, seen most of the time, with timezone
    row: ModifiedRow = {FIELD_DATE_TESTED: date_time.strftime("%Y-%m-%d %H:%M:%S %Z")}
    assert centre_file.is_valid_date_format(row, 1, FIELD_DATE_TESTED) == (
        True,
        {
            "year": f"{date_time.year:02}",
            "month": f"{date_time.month:02}",
            "day": f"{date_time.day:02}",
            "time": date_time.strftime("%H:%M:%S"),
            "timezone_name": date_time.tzname(),
        },
    )

    # "Normal" datetime, seen most of the time, without timezone
    row = {FIELD_DATE_TESTED: date_time.strftime("%Y-%m-%d %H:%M:%S")}
    assert centre_file.is_valid_date_format(row, 1, FIELD_DATE_TESTED) == (
        True,
        {
            "year": f"{date_time.year:02}",
            "month": f"{date_time.month:02}",
            "day": f"{date_time.day:02}",
            "time": date_time.strftime("%H:%M:%S"),
            "timezone_name": None,
        },
    )

    # other format
    row = {FIELD_DATE_TESTED: date_time.strftime("%d/%m/%Y %H:%M")}
    assert centre_file.is_valid_date_format(row, 1, FIELD_DATE_TESTED) == (
        True,
        {
            "year": f"{date_time.year:02}",
            "month": f"{date_time.month:02}",
            "day": f"{date_time.day:02}",
            "time": date_time.strftime("%H:%M"),
        },
    )

    # wrong format
    row = {FIELD_DATE_TESTED: date_time.strftime("%d %m %Y %H:%M")}
    assert centre_file.is_valid_date_format(row, 1, FIELD_DATE_TESTED) == (False, {})

    # empty date
    row = {FIELD_DATE_TESTED: ""}
    assert centre_file.is_valid_date_format(row, 1, FIELD_DATE_TESTED) == (True, {})


def test_convert_datetime_string_to_datetime():
    date_dict = {
        "year": "2020",
        "month": "11",
        "day": "22",
    }
    time_dict = {"hour": "4", "minute": "36", "second": "38"}
    time_with_seconds = f"{int(time_dict['hour']):02}:{time_dict['minute']}:{time_dict['second']}"
    time_without_seconds = f"{int(time_dict['hour']):02}:{time_dict['minute']}"

    date_time = datetime(
        **{key: int(value) for key, value in date_dict.items()},  # type: ignore
        **{key: int(value) for key, value in time_dict.items()},  # type: ignore
    )
    #  date and time with seconds with UTC timezone
    assert CentreFile.convert_datetime_string_to_datetime(
        **date_dict, time=time_with_seconds, timezone_name="UTC"
    ) == date_time.replace(tzinfo=timezone.utc)

    #  date and time with seconds with GMT timezone
    assert (
        CentreFile.convert_datetime_string_to_datetime(**date_dict, time=time_with_seconds, timezone_name="GMT")
        == date_time
    )

    #  date and time with seconds with no timezone
    assert CentreFile.convert_datetime_string_to_datetime(**date_dict, time=time_with_seconds) == date_time

    #  date and time with no seconds with UTC timezone
    assert CentreFile.convert_datetime_string_to_datetime(
        **date_dict, time=time_without_seconds, timezone_name="UTC"
    ) == date_time.replace(second=0, tzinfo=timezone.utc)

    #  date and time with no seconds with no timezone
    assert CentreFile.convert_datetime_string_to_datetime(**date_dict, time=time_without_seconds) == date_time.replace(
        second=0
    )
