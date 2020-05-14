import logging
import logging.config
import sys
from typing import List

import pymongo
from pymongo.errors import BulkWriteError

from crawler.config.logging import LOGGING_CONF  # type: ignore
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
    copy_collection,
    create_import_record,
    create_mongo_client,
    get_mongo_collection,
    get_mongo_db,
    populate_collection,
)
from crawler.helpers import (
    clean_up,
    download_csv_files,
    get_config,
    merge_daily_files,
    parse_csv,
    upload_file_to_sftp,
)

logging.config.dictConfig(LOGGING_CONF)
logger = logging.getLogger(__name__)


def run(sftp: bool, settings_module: str = "") -> None:
    try:
        config = get_config(settings_module)
        centres = config.CENTRES  # type: ignore

        if config is None:
            sys.exit("Config not found")

        with create_mongo_client(config) as client:
            db = get_mongo_db(config, client)

            centres_collection = get_mongo_collection(db, COLLECTION_CENTRES)

            logger.debug(
                f"Creating index '{FIELD_CENTRE_NAME}' on '{centres_collection.full_name}'"
            )
            centres_collection.create_index(FIELD_CENTRE_NAME, unique=True)
            populate_collection(centres_collection, centres, FIELD_CENTRE_NAME)

            imports_collection = get_mongo_collection(db, COLLECTION_IMPORTS)
            samples_collection = get_mongo_collection(db, COLLECTION_SAMPLES)

            # create a copy of the existing samples
            if samples_collection.estimated_document_count() > 0:
                logger.info(f"'{COLLECTION_SAMPLES}' collection is not empty so creating a copy")
                copy_collection(db, samples_collection)

                logger.info(f"Removing all documents from '{COLLECTION_SAMPLES}'")
                result = samples_collection.delete_many({})
                logger.debug(f"{result.deleted_count} records deleted")

            # create indices
            samples_collection.drop_indexes()
            logger.debug(
                f"Creating index '{FIELD_PLATE_BARCODE}' on '{samples_collection.full_name}'"
            )
            samples_collection.create_index(FIELD_PLATE_BARCODE)
            logger.debug(f"Creating compund index on '{samples_collection.full_name}'")
            # create compound index on 'Root Sample ID', 'RNA ID', 'Result', 'Lab ID' - some data
            #   had the same plate tested at another time so ignore the data if it is exactly the
            #   same
            samples_collection.create_index(
                [
                    (FIELD_ROOT_SAMPLE_ID, pymongo.ASCENDING),
                    (FIELD_RNA_ID, pymongo.ASCENDING),
                    (FIELD_RESULT, pymongo.ASCENDING),
                    (FIELD_RNA_ID, pymongo.ASCENDING),
                    (FIELD_LAB_ID, pymongo.ASCENDING),
                ],
                unique=True,
            )

            for centre in centres:
                logger.debug(f"Processing {centre['name']}")

                docs_inserted: int = 0
                latest_file_name: str = ""
                errors: List[str] = []
                try:
                    if sftp:
                        download_csv_files(config, centre)

                    if "merge_required" in centre.keys() and centre["merge_required"]:
                        master_file_name = merge_daily_files(centre)

                        # only upload to SFTP if config explicitly says so - this is to prevent
                        #   accidental uploads from non-production envs
                        if config.SFTP_UPLOAD:
                            upload_file_to_sftp(config, centre, master_file_name)

                    latest_file_name, errors, docs_to_insert = parse_csv(centre)

                    logger.debug(f"Attempting to insert {len(docs_to_insert)} docs")
                    result = samples_collection.insert_many(docs_to_insert, ordered=False)

                    docs_inserted = len(result.inserted_ids)
                except BulkWriteError as e:
                    # This is happening when there are duplicates in the data and the index prevents
                    #   the records from being written
                    logger.warning(e)
                    docs_inserted = e.details["nInserted"]
                    write_errors = {write_error["code"] for write_error in e.details["writeErrors"]}
                    for error in write_errors:
                        num_errors = len(
                            list(filter(lambda x: x["code"] == error, e.details["writeErrors"]))
                        )
                        errors.append(f"{num_errors} records with error code {error}")
                except Exception as e:
                    logger.exception(e)
                finally:
                    clean_up(centre)

                    logger.info(f"{docs_inserted} documents inserted")
                    # write status record
                    _ = create_import_record(
                        imports_collection, centre, docs_inserted, latest_file_name, errors,
                    )

        logger.info("Import complete")
    except Exception as e:
        logger.exception(e)
