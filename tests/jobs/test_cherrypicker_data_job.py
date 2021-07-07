from bson.objectid import ObjectId
from collections import namedtuple
from contextlib import ExitStack
from datetime import datetime
import json
import pytest
from unittest.mock import patch

from crawler.constants import (
    COLLECTION_CHERRYPICK_TEST_DATA,
    FIELD_CREATED_AT,
    FIELD_UPDATED_AT,
    FIELD_STATUS,
    FIELD_PLATE_SPECS,
    FIELD_BARCODES,
    FIELD_FAILURE_REASON,
    FIELD_STATUS_PENDING,
    FIELD_STATUS_STARTED,
    FIELD_STATUS_PREPARING_DATA,
    FIELD_STATUS_CRAWLING_DATA,
    FIELD_STATUS_COMPLETED,
    FIELD_STATUS_FAILED,
    TEST_DATA_CENTRE_PREFIX,
    TEST_DATA_ERROR_NO_RUN_FOR_ID,
    TEST_DATA_ERROR_WRONG_STATE,
    TEST_DATA_ERROR_NUMBER_OF_PLATES,
    TEST_DATA_ERROR_NUMBER_OF_POS_SAMPLES,
)
from crawler.db.mongo import get_mongo_collection
from crawler.jobs.cherrypicker_test_data import (
    TestDataError,
    process,
)
from tests.conftest import is_found_in_list


partial_pending_run = {
    FIELD_CREATED_AT: datetime(2012, 3, 4, 5, 6, 7, 890),
    FIELD_UPDATED_AT: datetime(2012, 3, 4, 5, 6, 7, 890),
    FIELD_STATUS: FIELD_STATUS_PENDING,
    FIELD_PLATE_SPECS: "[[2,48],[1,0],[1,96]]"
}

mocked_utc_now = datetime(2021, 3, 12, 9, 41, 0)

created_barcodes = ["Plate-1", "Plate-2", "Plate-3", "Plate-4"]

created_csv_rows = [
    ["Test", "CSV", "Data"],
    ["One",  "Two", "Three"]
]

created_barcode_metadata = [
    ["Plate-1", "positive samples: 48"],
    ["Plate-2", "positive samples: 48"],
    ["Plate-3", "positive samples: 0"],
    ["Plate-4", "positive samples: 96"],
]

LoggerMessages = namedtuple("LoggerMessages", ["info", "debug"])


@pytest.fixture
def logger_messages():
    with patch("crawler.jobs.cherrypicker_test_data.logger") as logger:
        infos = []
        logger.info.side_effect = lambda msg: infos.append(msg)

        debugs = []
        logger.debug.side_effect = lambda msg: debugs.append(msg)

        yield LoggerMessages(info=infos, debug=debugs)


@pytest.fixture
def mongo_collection(mongo_database):
    config, mongo_database = mongo_database

    yield config, get_mongo_collection(mongo_database, COLLECTION_CHERRYPICK_TEST_DATA)


@pytest.fixture
def mock_stack():
    with ExitStack() as stack:
        datetime_mock = stack.enter_context(patch("crawler.jobs.cherrypicker_test_data.datetime"))
        datetime_mock.utcnow.return_value = mocked_utc_now
        stack.enter_context(patch("crawler.jobs.cherrypicker_test_data.create_barcodes", return_value=created_barcodes))
        stack.enter_context(patch("crawler.jobs.cherrypicker_test_data.create_csv_rows", return_value=created_csv_rows))
        stack.enter_context(patch("crawler.jobs.cherrypicker_test_data.write_plates_file"))
        stack.enter_context(patch("crawler.jobs.cherrypicker_test_data.run_crawler"))
        stack.enter_context(patch("crawler.jobs.cherrypicker_test_data.create_barcode_meta", return_value=created_barcode_metadata))

        yield stack


def insert_pending_run(collection, plate_specs):
    result = collection.insert_one({**partial_pending_run, "plate_specs": plate_specs})

    return result.inserted_id


def get_run_doc(collection, run_id):
    return collection.find_one(ObjectId(run_id))


def test_process_success(logger_messages, mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_pending_run(collection, "[[2,48],[1,0],[1,96]]")

    barcode_meta = process(pending_id, config)
    run_doc = get_run_doc(collection, pending_id)

    assert barcode_meta == created_barcode_metadata
    assert run_doc[FIELD_UPDATED_AT] != partial_pending_run[FIELD_UPDATED_AT]
    assert run_doc[FIELD_STATUS] == FIELD_STATUS_COMPLETED
    assert run_doc[FIELD_BARCODES] == json.dumps(created_barcode_metadata)
    assert FIELD_FAILURE_REASON not in run_doc
    assert is_found_in_list("Begin generating", logger_messages.info)


def test_process_updates_through_statuses(logger_messages, mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_pending_run(collection, "[[2,48],[1,0],[1,96]]")

    with patch("crawler.jobs.cherrypicker_test_data.update_status") as update_status:
        process(pending_id, config)

        assert update_status.called_with(status=FIELD_STATUS_STARTED)
        assert update_status.called_with(status=FIELD_STATUS_PREPARING_DATA)
        assert update_status.called_with(status=FIELD_STATUS_CRAWLING_DATA)
        assert update_status.called_with(status=FIELD_STATUS_COMPLETED)
        assert update_status.not_called_with(status=FIELD_STATUS_FAILED)
