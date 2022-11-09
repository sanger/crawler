import json
import logging
from datetime import datetime
from functools import reduce
from typing import List, Optional, Tuple, cast

from bson.objectid import ObjectId
from lab_share_lib.config_readers import get_config
from pymongo.collection import Collection

from crawler.constants import (
    COLLECTION_CHERRYPICK_TEST_DATA,
    FIELD_BARCODES,
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
    TEST_DATA_ERROR_INVALID_PLATE_SPECS,
    TEST_DATA_ERROR_NO_RUN_FOR_ID,
    TEST_DATA_ERROR_NUMBER_OF_PLATES,
    TEST_DATA_ERROR_NUMBER_OF_POS_SAMPLES,
    TEST_DATA_ERROR_WRONG_STATE,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.exceptions import CherrypickerDataError
from crawler.helpers.cherrypicker_test_data import create_barcode_meta, create_barcodes, create_plate_messages
from crawler.processing.cptd_processor import CPTDProcessor
from crawler.types import Config

logger = logging.getLogger(__name__)


def process(run_id: str, config: Optional[Config] = None) -> List[List[str]]:
    """Generates cherrypicker test data for processing by Crawler and then
    processes it via the usual runner.

    The specification of the plates to be generated should be in Mongo. Each
    plate will contain an exact number of positive results between 0 and 96 as
    specified. Up to 200 plates can be generated at a time.

    Arguments:
        run_id: str - The ID of the run.  If this is not found in Mongo an
            exception will be thrown.

    Returns:
        Metadata about the plates generated, as:
        [ [ "barcode1", "description1" ], [ "barcode2", "description2" ] ]
    """
    logger.info("Begin generating data.")

    if config is None:
        config, _ = cast(Tuple[Config, str], get_config())

    with create_mongo_client(config) as mongo_client:
        mongo_db = get_mongo_db(config, mongo_client)
        collection = get_mongo_collection(mongo_db, COLLECTION_CHERRYPICK_TEST_DATA)

        return process_run(config, collection, run_id)


def process_run(config: Config, collection: Collection, run_id: str) -> List[List[str]]:
    dt = datetime.utcnow()
    test_data_processor = CPTDProcessor(config)
    run_doc = get_run_doc(collection, run_id)

    if run_doc[FIELD_STATUS] != FIELD_STATUS_PENDING:
        raise CherrypickerDataError(f"{TEST_DATA_ERROR_WRONG_STATE} '{FIELD_STATUS_PENDING}'")

    try:
        plate_specs, num_plates = validate_plate_specs(
            run_doc.get(FIELD_PLATE_SPECS), config.MAX_PLATES_PER_TEST_DATA_RUN
        )

        update_status(collection, run_id, FIELD_STATUS_STARTED)
        barcodes = create_barcodes(config, num_plates)

        update_status(collection, run_id, FIELD_STATUS_PREPARING_DATA)
        messages = create_plate_messages(plate_specs, dt, barcodes)

        update_status(collection, run_id, FIELD_STATUS_CRAWLING_DATA)
        test_data_processor.process(messages)

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
        raise CherrypickerDataError(f"{TEST_DATA_ERROR_NO_RUN_FOR_ID} '{run_id}'")
    logger.debug(f"Found run: {run_doc}")

    return run_doc


def validate_plate_specs(plate_specs, max_plates_per_run):
    if (
        type(plate_specs) != list
        or len(plate_specs) == 0
        or not all([type(ps) == list and len(ps) == 2 for ps in plate_specs])
        or not all([type(s) == int for ps in plate_specs for s in ps])
    ):
        raise CherrypickerDataError(TEST_DATA_ERROR_INVALID_PLATE_SPECS)

    num_plates = reduce(lambda a, b: a + int(b[0]), plate_specs, 0)
    if num_plates < 1 or num_plates > max_plates_per_run:
        raise CherrypickerDataError(TEST_DATA_ERROR_NUMBER_OF_PLATES.format(max_plates_per_run))

    positives_per_plate = [spec[1] for spec in plate_specs]
    if any([positives < 0 or positives > 96 for positives in positives_per_plate]):
        raise CherrypickerDataError(TEST_DATA_ERROR_NUMBER_OF_POS_SAMPLES)

    return plate_specs, num_plates


def update_status(collection, run_id, status):
    update_run(collection, ObjectId(run_id), {FIELD_STATUS: status})


def update_run(collection, run_id, update):
    update_dict = {"$set": update, "$currentDate": {FIELD_EVE_UPDATED: True}}
    collection.update_one({FIELD_MONGODB_ID: ObjectId(run_id)}, update_dict)
