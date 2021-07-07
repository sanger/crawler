from collections import namedtuple
from http import HTTPStatus
import pytest
from unittest.mock import patch

from crawler.constants import (
    FIELD_STATUS_COMPLETED,
    FLASK_ERROR_UNEXPECTED,
    FLASK_ERROR_MISSING_PARAMETERS,
)
from tests.conftest import is_found_in_list

barcode_metadata = [
    ["Plate-1", "positive samples: 30"],
    ["Plate-2", "positive samples: 50"],
]

LoggerMessages = namedtuple("LoggerMessages", ["info", "error", "exception"])


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
def process_mock():
    with patch("crawler.blueprints.cherrypicker_test_data.process") as process:
        yield process


def test_generate_endpoint_success(client, logger_messages, process_mock):
    process_mock.return_value = barcode_metadata
    test_run_id = "0123456789abcdef01234567"
    response = client.post("/cherrypick-test-data", json={"run_id": test_run_id})
    assert response.status_code == HTTPStatus.OK
    assert response.json["run_id"] == test_run_id
    assert response.json["plates"] == barcode_metadata
    assert response.json["status"] == FIELD_STATUS_COMPLETED
    assert "timestamp" in response.json
    assert "errors" not in response.json
    assert is_found_in_list("Generating test data", logger_messages.info)


@pytest.mark.parametrize("json", [{}, {"run_id": None}])
def test_generate_endpoint_invalid_json(json, client, logger_messages):
    response = client.post("/cherrypick-test-data", json=json)
    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert "run_id" not in response.json
    assert "plates" not in response.json
    assert "status" not in response.json
    assert "timestamp" in response.json
    assert is_found_in_list(FLASK_ERROR_MISSING_PARAMETERS, response.json["errors"])
    assert is_found_in_list(FLASK_ERROR_MISSING_PARAMETERS, logger_messages.error)
