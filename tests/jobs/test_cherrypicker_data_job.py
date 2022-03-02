import json
import os
from collections import namedtuple
from contextlib import ExitStack
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId

from crawler.config.centres import CENTRE_KEY_LAB_ID_DEFAULT, CENTRE_KEY_PREFIX
from crawler.constants import (
    COLLECTION_CHERRYPICK_TEST_DATA,
    FIELD_ADD_TO_DART,
    FIELD_BARCODES,
    FIELD_EVE_CREATED,
    FIELD_EVE_UPDATED,
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
    TEST_DATA_CENTRE_PREFIX,
    TEST_DATA_ERROR_INVALID_PLATE_SPECS,
    TEST_DATA_ERROR_NO_RUN_FOR_ID,
    TEST_DATA_ERROR_NUMBER_OF_PLATES,
    TEST_DATA_ERROR_NUMBER_OF_POS_SAMPLES,
    TEST_DATA_ERROR_WRONG_STATE,
)
from crawler.db.mongo import get_mongo_collection
from crawler.helpers.general_helpers import is_found_in_list
from crawler.jobs.cherrypicker_test_data import (
    CherrypickerDataError,
    get_run_doc,
    prepare_data,
    process,
    process_run,
    update_run,
    update_status,
    validate_plate_specs,
)

partial_run_doc = {
    FIELD_EVE_CREATED: datetime(2012, 3, 4, 5, 6, 7, 890),
    FIELD_EVE_UPDATED: datetime(2012, 3, 4, 5, 6, 7, 890),
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


def insert_run(collection, status=FIELD_STATUS_PENDING, plate_specs=((75, 48), (50, 0), (50, 96)), add_to_dart=False):
    run_doc = {**partial_run_doc, FIELD_STATUS: status}

    if plate_specs is not None:
        run_doc[FIELD_PLATE_SPECS] = plate_specs

    if add_to_dart is not None:
        run_doc[FIELD_ADD_TO_DART] = add_to_dart

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
    assert run_doc[FIELD_EVE_UPDATED] != partial_run_doc[FIELD_EVE_UPDATED]
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

    with pytest.raises(CherrypickerDataError) as e_info:
        process_run(config, collection, pending_id)

    assert TEST_DATA_ERROR_WRONG_STATE in str(e_info.value)
    assert FIELD_STATUS_PENDING in str(e_info.value)


def test_process_run_calls_helper_methods(mongo_collection, mock_stack):
    config, collection = mongo_collection
    plate_specs = [[1, 40], [5, 60], [5, 40]]
    pending_id = insert_run(collection, plate_specs=plate_specs)
    run_doc = get_doc(collection, pending_id)

    with ExitStack() as stack:
        get_run_doc = stack.enter_context(
            patch("crawler.jobs.cherrypicker_test_data.get_run_doc", return_value=run_doc)
        )
        validate_plate_specs = stack.enter_context(
            patch("crawler.jobs.cherrypicker_test_data.validate_plate_specs", return_value=(plate_specs, 6))
        )
        prepare_data = stack.enter_context(patch("crawler.jobs.cherrypicker_test_data.prepare_data"))

        process_run(config, collection, pending_id)

    get_run_doc.assert_called_once_with(collection, pending_id)
    validate_plate_specs.assert_called_once_with(plate_specs, config.MAX_PLATES_PER_TEST_DATA_RUN)
    prepare_data.assert_called_once_with(plate_specs, mocked_utc_now, created_barcodes, config)


def test_process_run_run_asks_for_correct_number_of_barcodes(mongo_collection, mock_stack):
    config, collection = mongo_collection
    pending_id = insert_run(collection, plate_specs=[[5, 10], [15, 20], [19, 30]])

    with patch("crawler.jobs.cherrypicker_test_data.create_barcodes") as create_barcodes:
        process_run(config, collection, pending_id)

    create_barcodes.assert_called_with(config, 5 + 15 + 19)


@pytest.mark.parametrize(
    "add_to_dart, expected_dart_value",
    [
        [None, False],
        [True, True],
        [False, False],
        [" tRuE  ", True],
        ["  FaLsE ", False],
        [["True"], False],
        [{"True": True}, False],
    ],
)
def test_process_run_calls_run_crawler_with_correct_parameters(
    mongo_collection, mock_stack, add_to_dart, expected_dart_value
):
    config, collection = mongo_collection
    pending_id = insert_run(collection, add_to_dart=add_to_dart)

    with patch("crawler.jobs.cherrypicker_test_data.run_crawler") as run_crawler:
        process_run(config, collection, pending_id)

    run_crawler.assert_called_with(
        sftp=False, keep_files=False, add_to_dart=expected_dart_value, centre_prefix=TEST_DATA_CENTRE_PREFIX
    )


@pytest.mark.parametrize(
    "plate_specs, expected_num",
    [
        [[[2, 0]], 2],
        [[[1, 96]], 1],
        [[[1, 1], [2, 2], [3, 3], [4, 4]], 10],
    ],
)
def test_validate_plate_specs_correct_extracts_specs_and_plate_number(plate_specs, expected_num):
    actual_specs, actual_num = validate_plate_specs(plate_specs, 200)

    assert actual_specs == plate_specs
    assert actual_num == expected_num


@pytest.mark.parametrize("bad_plate_specs", [None, [], [[1, 2, 3]], ["test"], [[1, "test"]], [[1, 40, "test"]]])
def test_validate_plate_specs_raises_error_invalid_plate_specs(bad_plate_specs):
    with pytest.raises(CherrypickerDataError) as e_info:
        validate_plate_specs(bad_plate_specs, 200)

    assert TEST_DATA_ERROR_INVALID_PLATE_SPECS in str(e_info.value)


@pytest.mark.parametrize(
    "bad_plate_specs",
    [
        [[0, 96]],  # 0 plates
        [[67, 10], [67, 20], [67, 30]],  # 201 plates
    ],
)
def test_validate_plate_specs_raises_error_wrong_number_of_plates(bad_plate_specs):
    with pytest.raises(CherrypickerDataError) as e_info:
        validate_plate_specs(bad_plate_specs, 200)

    error_msg = TEST_DATA_ERROR_NUMBER_OF_PLATES.format(200)
    assert error_msg in str(e_info.value)


@pytest.mark.parametrize("bad_plate_specs", [[[1, -1]], [[1, 97]]])
def test_validate_plate_specs_raises_error_invalid_num_of_positives(bad_plate_specs):
    with pytest.raises(CherrypickerDataError) as e_info:
        validate_plate_specs(bad_plate_specs, 200)

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

    with pytest.raises(CherrypickerDataError) as e_info:
        get_run_doc(collection, search_id)

    assert TEST_DATA_ERROR_NO_RUN_FOR_ID in str(e_info.value)
    assert search_id in str(e_info.value)


def test_prepare_data_calls_create_csv_rows_with_correct_parameters(config, mock_stack):
    plate_specs = [[5, 10], [15, 20], [19, 30]]

    with patch("crawler.jobs.cherrypicker_test_data.create_csv_rows") as create_csv_rows:
        prepare_data(plate_specs, mocked_utc_now, created_barcodes, config)

    test_centre = next(filter(lambda c: c[CENTRE_KEY_PREFIX] == TEST_DATA_CENTRE_PREFIX, config.CENTRES))
    create_csv_rows.assert_called_with(
        plate_specs, mocked_utc_now, created_barcodes, test_centre[CENTRE_KEY_LAB_ID_DEFAULT]
    )


def test_prepare_data_calls_write_plates_file_with_correct_parameters(config, mock_stack):
    plate_specs = [[5, 10], [15, 20], [19, 30]]
    with patch("crawler.jobs.cherrypicker_test_data.write_plates_file") as write_plates_file:
        prepare_data(plate_specs, mocked_utc_now, created_barcodes, config)

    plates_path = os.path.join(config.DIR_DOWNLOADED_DATA, TEST_DATA_CENTRE_PREFIX)
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
        {"$set": update_dict, "$currentDate": {FIELD_EVE_UPDATED: True}},
    )
