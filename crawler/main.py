import logging
import logging.config
import time

import pymongo

from crawler.constants import (
    COLLECTION_CENTRES,
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    FIELD_BARCODE,
    FIELD_CENTRE_NAME,
    FIELD_LAB_ID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
)
from crawler.db.mongo import (
    create_mongo_client,
    get_mongo_collection,
    get_mongo_db,
    populate_collection,
    samples_collection_accessor,
)
from crawler.priority_samples_process import update_priority_samples
from crawler.file_processing import Centre
from crawler.helpers.general_helpers import get_config

logger = logging.getLogger(__name__)


def run(sftp: bool, keep_files: bool, add_to_dart: bool, settings_module: str = "") -> None:
    try:
        start = time.time()
        config, settings_module = get_config(settings_module)

        logging.config.dictConfig(config.LOGGING)

        logger.info("-" * 80)
        logger.info("START")
        logger.info(f"Using settings from {settings_module}")

        centres = config.CENTRES

        with create_mongo_client(config) as client:
            db = get_mongo_db(config, client)

            # get or create the centres collection
            centres_collection = get_mongo_collection(db, COLLECTION_CENTRES)

            logger.debug(f"Creating index '{FIELD_CENTRE_NAME}' on '{centres_collection.full_name}'")
            centres_collection.create_index(FIELD_CENTRE_NAME, unique=True)
            populate_collection(centres_collection, centres, FIELD_CENTRE_NAME)

            # get or create the source plates collection
            source_plates_collection = get_mongo_collection(db, COLLECTION_SOURCE_PLATES)

            logger.debug(f"Creating index '{FIELD_BARCODE}' on '{source_plates_collection.full_name}'")
            source_plates_collection.create_index(FIELD_BARCODE, unique=True)

            logger.debug(f"Creating index '{FIELD_LH_SOURCE_PLATE_UUID}' on '{source_plates_collection.full_name}'")
            source_plates_collection.create_index(FIELD_LH_SOURCE_PLATE_UUID, unique=True)

            with samples_collection_accessor(db, COLLECTION_SAMPLES) as samples_collection:
                # Index on plate barcode to make it easier to select based on plate barcode
                logger.debug(f"Creating index '{FIELD_PLATE_BARCODE}' on '{samples_collection.full_name}'")
                samples_collection.create_index(FIELD_PLATE_BARCODE)

                # Index on result column to make it easier to select the positives
                logger.debug(f"Creating index '{FIELD_RESULT}' on '{samples_collection.full_name}'")
                samples_collection.create_index(FIELD_RESULT)

                # Index on unique combination of columns
                logger.debug(f"Creating compound index on '{samples_collection.full_name}'")
                # create compound index on 'Root Sample ID', 'RNA ID', 'Result', 'Lab ID' - some
                # data had the same plate tested at another time so ignore the data if it is exactly
                # the same
                samples_collection.create_index(
                    [
                        (FIELD_ROOT_SAMPLE_ID, pymongo.ASCENDING),
                        (FIELD_RNA_ID, pymongo.ASCENDING),
                        (FIELD_RESULT, pymongo.ASCENDING),
                        (FIELD_LAB_ID, pymongo.ASCENDING),
                    ],
                    unique=True,
                )

                # Index on lh_source_plate_uuid column
                # Added to make lighthouse API source completion event call query more efficient
                logger.debug(f"Creating index '{FIELD_LH_SOURCE_PLATE_UUID}' on '{samples_collection.full_name}'")
                samples_collection.create_index(FIELD_LH_SOURCE_PLATE_UUID)

                centres_instances = [Centre(config, centre_config) for centre_config in centres]
                for centre_instance in centres_instances:
                    logger.info("*" * 80)
                    logger.info(f"Processing {centre_instance.centre_config['name']}")

                    try:
                        if sftp:
                            centre_instance.download_csv_files()

                        centre_instance.process_files(add_to_dart)
                    except Exception as e:
                        logger.error("An exception occured")
                        logger.error(f"Error in centre {centre_instance.centre_config['name']}")
                        logger.exception(e)
                    finally:
                        if not keep_files and centre_instance.is_download_dir_walkable:
                            centre_instance.clean_up()

                # Prioritisation of samples
                update_priority_samples(db, config, add_to_dart)

        logger.info(f"Import complete in {round(time.time() - start, 2)}s")
        logger.info("=" * 80)
    except Exception as e:
        logger.exception(e)
