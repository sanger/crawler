import logging
import logging.config
from typing import Dict

from crawler.config.logging import LOGGING_CONF  # type: ignore
from crawler.config_helpers import get_centre_details, get_config
from crawler.constants import COLLECTION_IMPORTS, COLLECTION_SAMPLES
from crawler.db import (
    copy_collection,
    create_import_record,
    create_mongo_client,
    get_mongo_collection,
    get_mongo_db,
)
from crawler.helpers import parse_csv

logging.config.dictConfig(LOGGING_CONF)
logger = logging.getLogger(__name__)


def run(test_db_config: Dict = None) -> None:
    if test_db_config is None:
        test_db_config = {}
    try:
        config = get_config(test_db_config)
        centres = get_centre_details(config)

        with create_mongo_client(config) as client:
            db = get_mongo_db(config, client)
            imports_collection = get_mongo_collection(db, COLLECTION_IMPORTS)
            samples_collection = get_mongo_collection(db, COLLECTION_SAMPLES)

            if samples_collection.estimated_document_count() > 0:
                logger.info(f"{COLLECTION_SAMPLES} collection is not empty so creating copy")
                copy_collection(db, samples_collection)

                logger.info(f"Removing all documents from {COLLECTION_SAMPLES}")
                result = samples_collection.delete_many({})
                logger.debug(f"{result.deleted_count} records deleted")

            for centre in centres:
                logger.debug(f"Processing {centre['name']}")

                try:
                    errors, docs_to_insert = parse_csv(centre)

                    logger.debug(f"Attempting to insert {len(docs_to_insert)} docs")
                    result = samples_collection.insert_many(docs_to_insert)
                    logger.debug(f"{len(result.inserted_ids)} documents inserted")

                    # write status record
                    # TODO create the status record somewhere that if something else fails it is
                    # still written
                    _ = create_import_record(
                        imports_collection, centre, len(result.inserted_ids), errors
                    )
                except Exception as e:
                    logger.exception(e)
                    logger.info("Continuing...")
                    continue
    except Exception as e:
        logger.exception(e)
