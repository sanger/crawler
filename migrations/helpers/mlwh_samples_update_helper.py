from contextlib import closing
from datetime import datetime

from crawler.constants import COLLECTION_SAMPLES, FIELD_CREATED_AT, MONGO_DATETIME_FORMAT
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.db.mysql import create_mysql_connection, run_mysql_executemany_query
from crawler.helpers.general_helpers import map_mongo_sample_to_mysql, set_is_current_on_mysql_samples
from crawler.sql_queries import SQL_MLWH_MULTIPLE_INSERT
from crawler.types import Config
from migrations.helpers.shared_helper import print_exception, valid_datetime_string


def update_mlwh_with_legacy_samples(config: Config, s_start_datetime: str = "", s_end_datetime: str = "") -> None:
    if not valid_datetime_string(s_start_datetime):
        print("Aborting run: Expected format of Start datetime is YYMMDD_HHmm")
        return

    if not valid_datetime_string(s_end_datetime):
        print("Aborting run: Expected format of End datetime is YYMMDD_HHmm")
        return

    start_datetime = datetime.strptime(s_start_datetime, MONGO_DATETIME_FORMAT)
    end_datetime = datetime.strptime(s_end_datetime, MONGO_DATETIME_FORMAT)

    if start_datetime > end_datetime:
        print("Aborting run: End datetime must be greater than Start datetime")
        return

    print(f"Starting MLWH update process with Start datetime {start_datetime} and End datetime {end_datetime}")

    try:
        mongo_docs_for_sql = []
        number_docs_found = 0

        # open connection mongo
        with create_mongo_client(config) as client:
            mongo_db = get_mongo_db(config, client)

            samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

            print("Selecting Mongo samples")

            # this should take everything from the cursor find into RAM memory (assuming you have
            # enough memory)
            mongo_docs = list(
                samples_collection.find({FIELD_CREATED_AT: {"$gte": start_datetime, "$lte": end_datetime}})
            )
            number_docs_found = len(mongo_docs)
            print(f"{number_docs_found} documents found in the mongo database between these timestamps")

            # convert mongo field values into MySQL format
            for doc in mongo_docs:
                mongo_docs_for_sql.append(map_mongo_sample_to_mysql(doc, copy_date=True))

            mysql_samples = set_is_current_on_mysql_samples(mongo_docs_for_sql)

        if number_docs_found > 0:
            print(f"Updating MLWH database for {len(mysql_samples)} sample documents")
            # create connection to the MLWH database
            with closing(create_mysql_connection(config, False)) as mlwh_conn:
                # execute sql query to insert/update timestamps into MLWH
                run_mysql_executemany_query(mlwh_conn, SQL_MLWH_MULTIPLE_INSERT, mysql_samples)
        else:
            print("No documents found for this timestamp range, nothing to insert or update in MLWH")

    except Exception:
        print_exception()
