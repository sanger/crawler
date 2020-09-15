import logging
import logging.config
import time
from typing import List

import pymongo

from crawler.constants import (
    COLLECTION_SAMPLES,
    FIELD_CREATED_AT,
    # FIELD_LAB_ID,
    # FIELD_PLATE_BARCODE,
    # FIELD_RESULT,
    # FIELD_RNA_ID,
    # FIELD_ROOT_SAMPLE_ID,
)
from crawler.db import (
    create_mongo_client,
    get_mongo_collection,
    get_mongo_db,
    create_mysql_connection,
    run_mysql_executemany_query,
)
from crawler.helpers import (
    map_mongo_to_sql_columns_with_timestamps,
    # get_config,
    # current_time,
)

logger = logging.getLogger(__name__)

def run(settings_module: str = "", start_timestamp: str = "", end_timestamp) -> None:
    start = time.time()
    logging.config.dictConfig(config.LOGGING)  # type: ignore

    logger.info("-" * 80)
    logger.info("STARTING LEGACY MLWH UPDATE")
    logger.info(f"Using settings from {settings_module}")

    # TODO: check timestamp strings are valid if present
    # TODO: check end > start if both present

    try:
        mongo_values = []
        # open connection mongo
        with create_mongo_client(config) as client:
            mongo_db = get_mongo_db(config, client)

            samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

            logger.info(
                f"Selecting Mongo samples'"
            )
            # select from mongo between timestamps (in a cursor)
            cursor = samples_collection.find(
            {
                FIELD_CREATED_AT:{
                    $gte:start_timestamp,
                    $lte:end_timestamp
                }
            })

            while(cursor.hasNext()):
                # if it runs out of documents in its local batch it will fetch more based on batchsize
                document = cursor.next()

                # build up values for mongo samples
                # TODO: any limit to array size here? db is 2gb+
                values.append(map_mongo_to_sql_columns_with_timestamps(doc))


        logger.info(
            f"Updating MLWH'"
        )
        # create connection to the MLWH database
        with create_mysql_connection(config, True) as mlwh_conn:

                # execute sql query to insert/update timestamps into MLWH
                run_mysql_executemany_query(mlwh_conn, sql_query, mongo_values)


    except Exception as e:
        logger.exception(e)

    finally:
        # close mongodb connection?

        logger.info(f"Update complete in {round(time.time() - start, 2)}s")
        logger.info("=" * 80)


