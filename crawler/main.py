import logging
import logging.config
import time
from typing import List

import pymongo

from crawler.constants import (
    COLLECTION_CENTRES,
    COLLECTION_IMPORTS,
    COLLECTION_SAMPLES,
    FIELD_CENTRE_NAME,
    FIELD_LAB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
)
from crawler.db import (
    create_mongo_client,
    get_mongo_collection,
    get_mongo_db,
    populate_centres_collection,
    samples_collection_accessor
)
from crawler.helpers import (
    get_config,
    current_time,
)
from crawler.file_processing import Centre

logger = logging.getLogger(__name__)


def run(sftp: bool, keep_files: bool, settings_module: str = "") -> None:
    try:
        start = time.time()
        config, settings_module = get_config(settings_module)

        logging.config.dictConfig(config.LOGGING)  # type: ignore

        logger.info("-" * 80)
        logger.info("START")
        logger.info(f"Using settings from {settings_module}")

        centres = config.CENTRES  # type: ignore

        with create_mongo_client(config) as client:
            db = get_mongo_db(config, client)

            centres_collection = get_mongo_collection(db, COLLECTION_CENTRES)

            logger.debug(
                f"Creating index '{FIELD_CENTRE_NAME}' on '{centres_collection.full_name}'"
            )
            centres_collection.create_index(FIELD_CENTRE_NAME, unique=True)
            populate_centres_collection(centres_collection, centres, FIELD_CENTRE_NAME)

            imports_collection = get_mongo_collection(db, COLLECTION_IMPORTS)

            with samples_collection_accessor(db, COLLECTION_SAMPLES) as samples_collection:
                logger.debug(
                    f"Creating index '{FIELD_PLATE_BARCODE}' on '{samples_collection.full_name}'"
                )
                samples_collection.create_index(FIELD_PLATE_BARCODE)
                logger.debug(f"Creating compound index on '{samples_collection.full_name}'")
                # create compound index on 'Root Sample ID', 'RNA ID', 'Result', 'Lab ID' - some data
                #   had the same plate tested at another time so ignore the data if it is exactly the
                #   same
                samples_collection.create_index(
                    [
                        (FIELD_ROOT_SAMPLE_ID, pymongo.ASCENDING),
                        (FIELD_RNA_ID, pymongo.ASCENDING),
                        (FIELD_RESULT, pymongo.ASCENDING),
                        (FIELD_LAB_ID, pymongo.ASCENDING),
                    ],
                    unique=True,
                )

                centres_instances = [Centre(config, centre_config) for centre_config in centres]
                for centre_instance in centres_instances:
                    logger.info("*" * 80)
                    logger.info(f"Processing {centre_instance.centre_config['name']}")

                    try:
                        if sftp:
                            centre_instance.download_csv_files()

                        centre_instance.process_files()
                    except Exception as e:
                        logger.error("An exception occured")
                        logger.error(f"Error in centre {centre_instance.centre_config['name']}")
                        logger.exception(e)
                    finally:
                        if not (keep_files):
                            centre_instance.clean_up()

        logger.info(f"Import complete in {round(time.time() - start, 2)}s")
        logger.info("=" * 80)
    except Exception as e:
        logger.exception(e)




