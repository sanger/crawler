from collections import namedtuple
from http import HTTPStatus
from unittest.mock import patch

import pytest

from crawler.constants import FIELD_STATUS_COMPLETED, FLASK_ERROR_MISSING_PARAMETERS, FLASK_ERROR_UNEXPECTED
from crawler.jobs.cherrypicker_test_data import TestDataError
from tests.conftest import is_found_in_list

barcode_metadata = [
    ["Plate-1", "positive samples: 30"],
    ["Plate-2", "positive samples: 50"],
]

LoggerMessages = namedtuple("LoggerMessages", ["info", "error"])


@pytest.fixture
def logger_messages():
    with patch("crawler.blueprints.cherrypicker_test_data.logger") as logger:
        infos = []
        logger.info.side_effect = lambda msg: infos.append(msg)

        errors = []
        logger.error.side_effect = lambda msg: errors.append(msg)

        yield LoggerMessages(info=infos, error=errors)


@pytest.fixture
def process_mock():
    with patch("crawler.blueprints.cherrypicker_test_data.process") as process:
        yield process


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


def test_generate_endpoint_handles_testdataerror_exception(client, logger_messages, process_mock):
    test_error_message = "Test Error!"
    test_error = TestDataError(test_error_message)
    process_mock.side_effect = test_error
    response = client.post("/cherrypick-test-data", json={"run_id": "test_id"})
    assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert "run_id" not in response.json
    assert "plates" not in response.json
    assert "status" not in response.json
    assert "timestamp" in response.json
    assert is_found_in_list(test_error_message, response.json["errors"])
    assert is_found_in_list(test_error_message, logger_messages.error)


def test_generate_endpoint_handles_generic_exception(client, logger_messages, process_mock):
    test_error = ConnectionError()
    process_mock.side_effect = test_error
    response = client.post("/cherrypick-test-data", json={"run_id": "test_id"})
    assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert "run_id" not in response.json
    assert "plates" not in response.json
    assert "status" not in response.json
    assert "timestamp" in response.json
    assert is_found_in_list(FLASK_ERROR_UNEXPECTED, response.json["errors"])
    assert is_found_in_list(type(test_error).__name__, response.json["errors"])
    assert is_found_in_list(FLASK_ERROR_UNEXPECTED, logger_messages.error)
    assert is_found_in_list(type(test_error).__name__, logger_messages.error)
