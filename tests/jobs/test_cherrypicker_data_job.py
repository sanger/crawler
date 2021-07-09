import json
import os
from collections import namedtuple
from contextlib import ExitStack
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId

from crawler.constants import (
    COLLECTION_CHERRYPICK_TEST_DATA,
    FIELD_BARCODES,
    FIELD_CREATED_AT,
    FIELD_FAILURE_REASON,
    FIELD_MONGODB_ID,
    FIELD_PLATE_SPECS,
    FIELD_STATUS,
    FIELD_STATUS_COMPLETED,
    FIELD_STATUS_CRAWLING_DATA,
    FIELD_STATUS_FAILED,
    FIELD_STATUS_PENDING,
    FIELD_STATUS_PREPARING_DATA,
    FIELD_STATUS_STARTED,
    FIELD_UPDATED_AT,
    TEST_DATA_CENTRE_PREFIX,
    TEST_DATA_ERROR_INVALID_PLATE_SPECS,
    TEST_DATA_ERROR_NO_RUN_FOR_ID,
    TEST_DATA_ERROR_NUMBER_OF_PLATES,
    TEST_DATA_ERROR_NUMBER_OF_POS_SAMPLES,
    TEST_DATA_ERROR_WRONG_STATE,
)
from crawler.db.mongo import get_mongo_collection
from crawler.jobs.cherrypicker_test_data import (
    TestDataError,
    extract_plate_specs,
    get_run_doc,
    prepare_data,
    process,
    process_run,
    update_run,
    update_status,
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


def insert_run(collection, status=FIELD_STATUS_PENDING, plate_specs="[[75, 48], [50, 0], [50, 96]]"):
    run_doc = {**partial_run_doc, FIELD_STATUS: status, FIELD_PLATE_SPECS: plate_specs}

    if plate_specs is None:
        del run_doc[FIELD_PLATE_SPECS]

    result = collection.insert_one(run_doc)

    return result.inserted_id


def get_doc(collection, run_id):
    return collection.find_one(ObjectId(run_id))


def test_process_success(logger_messages, config):
    pending_id = "000000000000000000000000"

    mongo_client = Mock()
    mongo_db = Mock()
    mongo_collection = Mock()

    with ExitStack() as stack:
        mongo_client_context = Mock()
        mongo_client_context.__enter__ = Mock(return_value=mongo_client)
        mongo_client_context.__exit__ = Mock()
        create_mongo_client = stack.enter_context(
            patch("crawler.jobs.cherrypicker_test_data.create_mongo_client", return_value=mongo_client_context)
        )
        get_mongo_db = stack.enter_context(
            patch("crawler.jobs.cherrypicker_test_data.get_mongo_db", return_value=mongo_db)
        )
        get_mongo_collection = stack.enter_context(
            patch("crawler.jobs.cherrypicker_test_data.get_mongo_collection", return_value=mongo_collection)
        )
        process_run = stack.enter_context(
            patch("crawler.jobs.cherrypicker_test_data.process_run", return_value=created_barcode_metadata)
        )

        barcode_meta = process(pending_id, config)

    assert is_found_in_list("Begin generating", logger_messages.info)
    create_mongo_client.assert_called_once_with(config)
    get_mongo_db.assert_called_once_with(config, mongo_client)
    get_mongo_collection.assert_called_once_with(mongo_db, COLLECTION_CHERRYPICK_TEST_DATA)
    process_run.assert_called_once_with(config, mongo_collection, pending_id)
    assert barcode_meta == created_barcode_metadata


def test_process_run_success(mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_run(collection)

    barcode_meta = process_run(config, collection, pending_id)
    run_doc = get_doc(collection, pending_id)

    assert barcode_meta == created_barcode_metadata
    assert run_doc[FIELD_UPDATED_AT] != partial_run_doc[FIELD_UPDATED_AT]
    assert run_doc[FIELD_STATUS] == FIELD_STATUS_COMPLETED
    assert run_doc[FIELD_BARCODES] == json.dumps(created_barcode_metadata)
    assert FIELD_FAILURE_REASON not in run_doc


def test_process_run_updates_through_statuses(mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_run(collection)

    with patch("crawler.jobs.cherrypicker_test_data.update_status") as update_status:
        with patch("crawler.jobs.cherrypicker_test_data.update_run") as update_run:
            process_run(config, collection, pending_id)

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
def test_process_run_raises_error_when_run_not_pending(mongo_collection, mock_stack, wrong_status):
    config, collection = mongo_collection
    pending_id = insert_run(collection, status=wrong_status)

    with pytest.raises(TestDataError) as e_info:
        process_run(config, collection, pending_id)

    assert TEST_DATA_ERROR_WRONG_STATE in str(e_info.value)
    assert FIELD_STATUS_PENDING in str(e_info.value)


def test_process_run_calls_helper_methods(mongo_collection, mock_stack):
    config, collection = mongo_collection
    plate_specs = [[1, 40], [5, 60]]
    plate_specs_string = json.dumps(plate_specs)
    pending_id = insert_run(collection, plate_specs=plate_specs_string)
    run_doc = get_doc(collection, pending_id)

    with ExitStack() as stack:
        get_run_doc = stack.enter_context(
            patch("crawler.jobs.cherrypicker_test_data.get_run_doc", return_value=run_doc)
        )
        extract_plate_specs = stack.enter_context(
            patch("crawler.jobs.cherrypicker_test_data.extract_plate_specs", return_value=(plate_specs, 6))
        )
        prepare_data = stack.enter_context(patch("crawler.jobs.cherrypicker_test_data.prepare_data"))

        process_run(config, collection, pending_id)

    get_run_doc.assert_called_once_with(collection, pending_id)
    extract_plate_specs.assert_called_once_with(plate_specs_string)
    prepare_data.assert_called_once_with(plate_specs, mocked_utc_now, created_barcodes, config.DIR_DOWNLOADED_DATA)


def test_process_run_handles_missing_plate_specs(mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_run(collection, plate_specs=None)

    try:
        with patch("crawler.jobs.cherrypicker_test_data.extract_plate_specs", return_value=([1, 96], 1)):
            process_run(config, collection, pending_id)
    except Exception as e:
        pytest.fail(f"Having no plate specs should not raise an exception, but this was raised:  {e}")


def test_process_run_run_asks_for_correct_number_of_barcodes(mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_run(collection, plate_specs="[[5, 10], [15, 20], [19, 30]]")

    with patch("crawler.jobs.cherrypicker_test_data.create_barcodes") as create_barcodes:
        process_run(config, collection, pending_id)

    create_barcodes.assert_called_with(config, 5 + 15 + 19)


def test_process_run_calls_run_crawler_with_correct_parameters(mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_run(collection)

    with patch("crawler.jobs.cherrypicker_test_data.run_crawler") as run_crawler:
        process_run(config, collection, pending_id)

    run_crawler.assert_called_with(sftp=False, keep_files=False, add_to_dart=False, centre_prefix="CPTD")


@pytest.mark.parametrize(
    "plate_specs_string, expected_specs, expected_num",
    [
        ["[[2,0]]", [[2, 0]], 2],
        ["[[1,96]]", [[1, 96]], 1],
        ["[[1,1],[2,2],[3,3],[4,4]]", [[1, 1], [2, 2], [3, 3], [4, 4]], 10],
    ],
)
def test_extract_plate_specs_correct_extracts_specs_and_plate_number(plate_specs_string, expected_specs, expected_num):
    actual_specs, actual_num = extract_plate_specs(plate_specs_string)

    assert actual_specs == expected_specs
    assert actual_num == expected_num


@pytest.mark.parametrize("bad_plate_specs", [None, ""])
def test_extract_plate_specs_raises_error_invalid_plate_specs(bad_plate_specs):
    with pytest.raises(TestDataError) as e_info:
        extract_plate_specs(bad_plate_specs)

    assert TEST_DATA_ERROR_INVALID_PLATE_SPECS in str(e_info.value)


@pytest.mark.parametrize(
    "bad_plate_specs",
    [
        "[]",  # Unspecified plates
        "[[0, 96]]",  # 0 plates
        "[[67, 10], [67, 20], [67, 30]]",  # 201 plates
    ],
)
def test_extract_plate_specs_raises_error_wrong_number_of_plates(bad_plate_specs):
    with pytest.raises(TestDataError) as e_info:
        extract_plate_specs(bad_plate_specs)

    assert TEST_DATA_ERROR_NUMBER_OF_PLATES in str(e_info.value)


@pytest.mark.parametrize("bad_plate_specs", ["[[1, -1]]", "[[1, 97]]"])
def test_extract_plate_specs_raises_error_invalid_num_of_positives(bad_plate_specs):
    with pytest.raises(TestDataError) as e_info:
        extract_plate_specs(bad_plate_specs)

    assert TEST_DATA_ERROR_NUMBER_OF_POS_SAMPLES in str(e_info.value)


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


def test_prepare_data_calls_create_csv_rows_with_correct_parameters(mock_stack):
    plate_specs = [[5, 10], [15, 20], [19, 30]]

    with patch("crawler.jobs.cherrypicker_test_data.create_csv_rows") as create_csv_rows:
        prepare_data(plate_specs, mocked_utc_now, created_barcodes, "")

    create_csv_rows.assert_called_with(plate_specs, mocked_utc_now, created_barcodes)


def test_prepare_data_calls_write_plates_file_with_correct_parameters(mock_stack):
    plate_specs = [[5, 10], [15, 20], [19, 30]]
    data_path = "a_path"
    with patch("crawler.jobs.cherrypicker_test_data.write_plates_file") as write_plates_file:
        prepare_data(plate_specs, mocked_utc_now, created_barcodes, data_path)

    plates_path = os.path.join(data_path, TEST_DATA_CENTRE_PREFIX)
    filename = "CPTD_210312_094100_000000.csv"
    write_plates_file.assert_called_with(created_csv_rows, plates_path, filename)


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
