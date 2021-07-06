from collections import namedtuple
from http import HTTPStatus
import pytest
from unittest.mock import patch

from crawler.constants import (
    FIELD_STATUS_COMPLETED,
    FLASK_ERROR_UNEXPECTED,
    FLASK_ERROR_MISSING_PARAMETERS,
)


LoggerMessages = namedtuple("LoggerMessages", ["info", "error", "exception"])

barcode_metadata = [
    ["Plate-1", "positive samples: 30"],
    ["Plate-2", "positive samples: 50"],
]


@pytest.fixture
def logger_messages():
    with patch("crawler.blueprints.cherrypicker_test_data.logger") as logger:
        infos = []
        logger.info.side_effect = lambda msg: infos.append(msg)

        errors = []
        logger.error.side_effect = lambda msg: errors.append(msg)

        exceptions = []
        logger.exception.side_effect = lambda msg: exceptions.append(msg)

        yield LoggerMessages(info=infos, error=errors, exception=exceptions)


@pytest.fixture
@patch("crawler.jobs.cherrypicker_test_data.process")
def process_mock(process):
    process.return_value = barcode_metadata
    yield process


# def test_generate_endpoint_success(client, logger_messages, process_mock):
#     response = client.post("/cherrypick-test-data", json={"run_id": "0123456789abcdef"})
#     assert response.status_code == HTTPStatus.BAD_REQUEST
#     assert FLASK_ERROR_MISSING_PARAMETERS in response.json


@pytest.mark.parametrize("json", [{}, {"run_id": None}])
def test_generate_endpoint_invalid_json(json, client, logger_messages):
    response = client.post("/cherrypick-test-data", json=json)
    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert 'errors' in response.json
    assert len(response.json['errors']) == 1
    assert FLASK_ERROR_MISSING_PARAMETERS in response.json['errors'][0]
