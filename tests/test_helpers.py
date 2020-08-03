import os
import pytest
from unittest.mock import patch

from crawler.constants import (
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
)
from crawler.helpers import (
    get_config,
)


def test_get_config():
    with pytest.raises(ModuleNotFoundError):
        get_config("x.y.z")
