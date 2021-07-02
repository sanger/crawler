from bson.objectid import ObjectId
from datetime import datetime
from functools import reduce
import json
import logging

from crawler.constants import (
    COLLECTION_CHERRYPICK_TEST_DATA,
    FIELD_MONGODB_ID,
    FIELD_UPDATED_AT,
    FIELD_STATUS,
    FIELD_PLATE_SPECS,
    FIELD_FAILURE_REASON,
    FIELD_STATUS_PENDING,
    FIELD_STATUS_STARTED,
    FIELD_STATUS_PREPARING_DATA,
    FIELD_STATUS_CRAWLING_DATA,
    FIELD_STATUS_COMPLETED,
    FIELD_STATUS_FAILED,
    FIELD_UPDATED_AT,
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
)
from crawler.helpers.general_helpers import get_config


logger = logging.getLogger(__name__)

class TestDataError(Exception):
    def __init__(self, message):
        self.message = message

def generate(run_id: str, settings_module = "") -> str:
    """Generates cherrypicker test data for processing by Crawler.

    The specification of the plates to be generated should be in Mongo.
    Each plate will contain an exact number of positive results between 0 and 96
    as specified. Up to 100 plates can be generated at a time.

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
            raise TestDataError(f"Run doesn't have status '{FIELD_STATUS_PENDING}'")

        plate_specs_string = run_doc[FIELD_PLATE_SPECS]
        plate_specs = json.loads(plate_specs_string)

        num_plates = reduce(lambda a, b: a + b[0], plate_specs, 0)
        if num_plates < 1 or num_plates > 100:
            log_processing_error(collection, run_id, "Number of plates to generate must be between 1 and 100")

        # TODO: Check no plates ask for fewer than 0 or more than 96 positives

        dt = datetime.utcnow()
        barcodes = create_barcodes(num_plates)
        csv_rows = create_csv_rows(plate_specs, dt, barcodes)
        # filename = write_file(dt, rows)

        update_status(collection, run_id, FIELD_STATUS_COMPLETED)
        return create_barcode_meta(plate_specs, barcodes)


def get_run_doc(collection, run_id):
    logger.info(f"Getting Mongo document for ID: {run_id}")

    run_doc = collection.find_one(ObjectId(run_id))
    if run_doc is None:
        raise TestDataError(f"No run found for ID {run_id}")
    logger.debug(f"Found run: {run_doc}")

    return run_doc


def log_processing_error(collection, run_id, message):
    update_run(collection, run_id, {
        FIELD_STATUS: FIELD_STATUS_FAILED,
        FIELD_FAILURE_REASON: message,
    })
    raise TestDataError(message)


def update_status(collection, run_id, status):
    update_run(collection, ObjectId(run_id), { FIELD_STATUS: status })


def update_run(collection, run_id, update):
    update_dict = { "$set": update, "$currentDate": { FIELD_UPDATED_AT: True } }
    collection.update_one({FIELD_MONGODB_ID: ObjectId(run_id) }, update_dict)
