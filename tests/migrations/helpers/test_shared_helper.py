from datetime import datetime

from migrations.helpers.shared_helper import valid_datetime_string

# ----- valid_datetime_string tests -----


def test_valid_datetime_string():
    result1 = valid_datetime_string("")
    assert result1 is False
    result2 = valid_datetime_string("201209_0000")
    assert result2 is True


