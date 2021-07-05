from bson.objectid import ObjectId
from datetime import datetime
from functools import reduce
import json
import logging
import os
from typing import List

from crawler.constants import (
    COLLECTION_CHERRYPICK_TEST_DATA,
    FIELD_MONGODB_ID,
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
from crawler.db.mongo import (
    create_mongo_client,
    get_mongo_collection,
    get_mongo_db,
)
from crawler.helpers.cherrypicker_test_data import (
    create_barcodes,
    create_barcode_meta,
    create_csv_rows,
    write_plates_file,
)
from crawler.helpers.general_helpers import get_config


logger = logging.getLogger(__name__)


class TestDataError(Exception):
    def __init__(self, message):
        self.message = message


def process(run_id: str, settings_module: str = "") -> List[List[str]]:
    """Generates cherrypicker test data for processing by Crawler and then
    processes it via the usual runner.

    The specification of the plates to be generated should be in Mongo. Each
    plate will contain an exact number of positive results between 0 and 96 as
    specified. Up to 100 plates can be generated at a time.

    Arguments:
        run_id: str - The ID of the run.  If this is not found in Mongo an
            exception will be thrown.

    Returns:
        Metadata about the plates generated, as:
        [ [ "barcode1", "description1" ], [ "barcode2", "description2" ] ]
    """
    logger.info("Begin generating data.")

    config, _ = get_config(settings_module)
    with create_mongo_client(config) as mongo_client:
        mongo_db = get_mongo_db(config, mongo_client)
        collection = get_mongo_collection(mongo_db, COLLECTION_CHERRYPICK_TEST_DATA)

        run_doc = get_run_doc(collection, run_id)
        if run_doc[FIELD_STATUS] != FIELD_STATUS_PENDING:
            raise TestDataError(f"{TEST_DATA_ERROR_WRONG_STATE} '{FIELD_STATUS_PENDING}'")

        try:
            plate_specs_string = run_doc[FIELD_PLATE_SPECS]
            plate_specs: List[List[int]] = json.loads(plate_specs_string)

            num_plates = reduce(lambda a, b: a + b[0], plate_specs, 0)
            if num_plates < 1 or num_plates > 100:
                raise TestDataError(TEST_DATA_ERROR_NUMBER_OF_PLATES)

            positives_per_plate = [spec[1] for spec in plate_specs]
            if any([positives < 0 or positives > 96 for positives in positives_per_plate]):
                raise TestDataError(TEST_DATA_ERROR_NUMBER_OF_POS_SAMPLES)

            update_status(collection, run_id, FIELD_STATUS_STARTED)

            dt = datetime.utcnow()
            barcodes = create_barcodes(num_plates)

            update_status(collection, run_id, FIELD_STATUS_PREPARING_DATA)

            csv_rows = create_csv_rows(plate_specs, dt, barcodes)
            plates_path = os.path.join(config.DIR_DOWNLOADED_DATA, TEST_DATA_CENTRE_PREFIX)
            plates_filename = f"{TEST_DATA_CENTRE_PREFIX}_{dt.strftime('%y%m%d_%H%M%S_%f')}.csv"
            write_plates_file(csv_rows, plates_path, plates_filename)

            update_status(collection, run_id, FIELD_STATUS_CRAWLING_DATA)

            # TODO: start the crawler for the test data centre

            barcode_meta = create_barcode_meta(plate_specs, barcodes)
            update_run(
                collection,
                run_id,
                {
                    FIELD_STATUS: FIELD_STATUS_COMPLETED,
                    FIELD_BARCODES: json.dumps(barcode_meta),
                },
            )

            return barcode_meta
        except Exception as e:
            update_run(
                collection,
                run_id,
                {
                    FIELD_STATUS: FIELD_STATUS_FAILED,
                    FIELD_FAILURE_REASON: str(e),
                },
            )
            raise


def get_run_doc(collection, run_id):
    logger.info(f"Getting Mongo document for ID: {run_id}")

    run_doc = collection.find_one(ObjectId(run_id))
    if run_doc is None:
        raise TestDataError(f"{TEST_DATA_ERROR_NO_RUN_FOR_ID} '{run_id}'")
    logger.debug(f"Found run: {run_doc}")

    return run_doc


def update_status(collection, run_id, status):
    update_run(collection, ObjectId(run_id), {FIELD_STATUS: status})


def update_run(collection, run_id, update):
    update_dict = {"$set": update, "$currentDate": {FIELD_UPDATED_AT: True}}
    collection.update_one({FIELD_MONGODB_ID: ObjectId(run_id)}, update_dict)
