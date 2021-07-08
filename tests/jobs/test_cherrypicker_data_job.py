from bson.objectid import ObjectId
from collections import namedtuple
from contextlib import ExitStack
from datetime import datetime
import json
import os
import pytest
from unittest.mock import patch

from crawler.constants import (
    COLLECTION_CHERRYPICK_TEST_DATA,
    FIELD_MONGODB_ID,
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
    TEST_DATA_ERROR_INVALID_PLATE_SPECS,
    TEST_DATA_ERROR_NO_RUN_FOR_ID,
    TEST_DATA_ERROR_WRONG_STATE,
    TEST_DATA_ERROR_NUMBER_OF_PLATES,
    TEST_DATA_ERROR_NUMBER_OF_POS_SAMPLES,
)
from crawler.db.mongo import get_mongo_collection
from crawler.jobs.cherrypicker_test_data import (
    TestDataError,
    process,
    get_run_doc,
    update_status,
    update_run,
)
from tests.conftest import is_found_in_list


partial_run_doc = {
    FIELD_CREATED_AT: datetime(2012, 3, 4, 5, 6, 7, 890),
    FIELD_UPDATED_AT: datetime(2012, 3, 4, 5, 6, 7, 890),
}

mocked_utc_now = datetime(2021, 3, 12, 9, 41, 0)

created_barcodes = ["Plate-1", "Plate-2", "Plate-3", "Plate-4"]

created_csv_rows = [["Test", "CSV", "Data"], ["One", "Two", "Three"]]

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
        stack.enter_context(
            patch("crawler.jobs.cherrypicker_test_data.create_barcode_meta", return_value=created_barcode_metadata)
        )

        yield stack


def insert_run(collection, status=FIELD_STATUS_PENDING, plate_specs="[[50, 48], [25, 0], [25, 96]]"):
    result = collection.insert_one({**partial_run_doc, FIELD_STATUS: status, FIELD_PLATE_SPECS: plate_specs})

    return result.inserted_id


def get_doc(collection, run_id):
    return collection.find_one(ObjectId(run_id))


def test_process_success(logger_messages, mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_run(collection)

    barcode_meta = process(pending_id, config)
    run_doc = get_doc(collection, pending_id)

    assert barcode_meta == created_barcode_metadata
    assert run_doc[FIELD_UPDATED_AT] != partial_run_doc[FIELD_UPDATED_AT]
    assert run_doc[FIELD_STATUS] == FIELD_STATUS_COMPLETED
    assert run_doc[FIELD_BARCODES] == json.dumps(created_barcode_metadata)
    assert FIELD_FAILURE_REASON not in run_doc
    assert is_found_in_list("Begin generating", logger_messages.info)


def test_process_updates_through_statuses(mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_run(collection)

    with patch("crawler.jobs.cherrypicker_test_data.update_status") as update_status:
        with patch("crawler.jobs.cherrypicker_test_data.update_run") as update_run:
            process(pending_id, config)

    update_status.assert_any_call(collection, pending_id, FIELD_STATUS_STARTED)
    update_status.assert_any_call(collection, pending_id, FIELD_STATUS_PREPARING_DATA)
    update_status.assert_any_call(collection, pending_id, FIELD_STATUS_CRAWLING_DATA)
    update_run.assert_any_call(
        collection,
        pending_id,
        {
            FIELD_STATUS: FIELD_STATUS_COMPLETED,
            FIELD_BARCODES: json.dumps(created_barcode_metadata),
        },
    )


@pytest.mark.parametrize(
    "wrong_status",
    [
        FIELD_STATUS_STARTED,
        FIELD_STATUS_PREPARING_DATA,
        FIELD_STATUS_CRAWLING_DATA,
        FIELD_STATUS_COMPLETED,
        FIELD_STATUS_FAILED,
    ],
)
def test_process_raises_error_when_run_not_pending(mongo_collection, mock_stack, wrong_status):
    config, collection = mongo_collection
    pending_id = insert_run(collection, status=wrong_status)

    with pytest.raises(TestDataError) as e_info:
        process(pending_id, config)

    assert TEST_DATA_ERROR_WRONG_STATE in str(e_info.value)
    assert FIELD_STATUS_PENDING in str(e_info.value)


@pytest.mark.parametrize("bad_plate_specs", [None, ""])
def test_process_raises_error_invalid_plate_specs(mongo_collection, mock_stack, bad_plate_specs):
    config, collection = mongo_collection
    pending_id = insert_run(collection, plate_specs=bad_plate_specs)

    with pytest.raises(TestDataError) as e_info:
        process(pending_id, config)

    assert TEST_DATA_ERROR_INVALID_PLATE_SPECS in str(e_info.value)


@pytest.mark.parametrize(
    "bad_plate_specs",
    [
        "[]",  # Unspecified plates
        "[[0, 96]]",  # 0 plates
        "[[34, 10], [34, 20], [33, 30]]",  # 101 plates
    ],
)
def test_process_raises_error_wrong_number_of_plates(mongo_collection, mock_stack, bad_plate_specs):
    config, collection = mongo_collection
    pending_id = insert_run(collection, plate_specs=bad_plate_specs)

    with pytest.raises(TestDataError) as e_info:
        process(pending_id, config)

    assert TEST_DATA_ERROR_NUMBER_OF_PLATES in str(e_info.value)


@pytest.mark.parametrize("bad_plate_specs", ["[[1, -1]]", "[[1, 97]]"])
def test_process_raises_error_invalid_num_of_positives(mongo_collection, mock_stack, bad_plate_specs):
    config, collection = mongo_collection
    pending_id = insert_run(collection, plate_specs=bad_plate_specs)

    with pytest.raises(TestDataError) as e_info:
        process(pending_id, config)

    assert TEST_DATA_ERROR_NUMBER_OF_POS_SAMPLES in str(e_info.value)


def test_process_asks_for_correct_number_of_barcodes(mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_run(collection, plate_specs="[[5, 10], [15, 20], [19, 30]]")

    with patch("crawler.jobs.cherrypicker_test_data.create_barcodes") as create_barcodes:
        process(pending_id, config)

    create_barcodes.assert_called_with(config, 5 + 15 + 19)


def test_process_calls_create_csv_rows_with_correct_parameters(mongo_collection, mock_stack):
    config, collection = mongo_collection
    plate_specs = [[5, 10], [15, 20], [19, 30]]
    plate_specs_string = json.dumps(plate_specs)
    pending_id = insert_run(collection, plate_specs=plate_specs_string)

    with patch("crawler.jobs.cherrypicker_test_data.create_csv_rows") as create_csv_rows:
        process(pending_id, config)

    create_csv_rows.assert_called_with(plate_specs, mocked_utc_now, created_barcodes)


def test_process_calls_write_plates_file_with_correct_parameters(mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_run(collection)

    with patch("crawler.jobs.cherrypicker_test_data.write_plates_file") as write_plates_file:
        process(pending_id, config)

    plates_path = os.path.join(config.DIR_DOWNLOADED_DATA, TEST_DATA_CENTRE_PREFIX)
    filename = "CPTD_210312_094100_000000.csv"
    write_plates_file.assert_called_with(created_csv_rows, plates_path, filename)


def test_process_calls_run_crawler_with_correct_parameters(mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_run(collection)

    with patch("crawler.jobs.cherrypicker_test_data.run_crawler") as run_crawler:
        process(pending_id, config)

    run_crawler.assert_called_with(sftp=False, keep_files=False, add_to_dart=False, centre_prefix="CPTD")


def test_get_run_doc_gets_the_doc_by_id(logger_messages, mongo_collection):
    _, collection = mongo_collection
    pending_id = insert_run(collection)

    actual = get_run_doc(collection, pending_id)
    expected = get_doc(collection, pending_id)

    assert actual == expected
    assert is_found_in_list("Getting Mongo document", logger_messages.info)
    assert is_found_in_list(str(pending_id), logger_messages.info)
    assert is_found_in_list("Found run", logger_messages.debug)
    assert is_found_in_list(str(actual), logger_messages.debug)


def test_get_run_doc_raises_error_when_id_not_found(mongo_collection):
    _, collection = mongo_collection
    insert_run(collection)
    search_id = "000000000000000000000000"

    with pytest.raises(TestDataError) as e_info:
        get_run_doc(collection, search_id)

    assert TEST_DATA_ERROR_NO_RUN_FOR_ID in str(e_info.value)
    assert search_id in str(e_info.value)


def test_update_status_calls_update_run_with_correct_parameters(mongo_collection):
    _, collection = mongo_collection
    test_id = "000000000000000000000000"
    test_status = "Test Status"

    with patch("crawler.jobs.cherrypicker_test_data.update_run") as update_run_mock:
        update_status(collection, test_id, test_status)

    update_run_mock.assert_called_once_with(collection, ObjectId(test_id), {FIELD_STATUS: test_status})


def test_update_run_calls_mongo_with_correct_parameters(mongo_collection):
    _, collection = mongo_collection
    test_id = "000000000000000000000000"
    update_dict = {"a_key": "a_value"}

    with patch("pymongo.collection.Collection.update_one") as update_one:
        update_run(collection, test_id, update_dict)

    update_one.assert_called_once_with(
        {FIELD_MONGODB_ID: ObjectId(test_id)},
        {"$set": update_dict, "$currentDate": {FIELD_UPDATED_AT: True}},
    )
