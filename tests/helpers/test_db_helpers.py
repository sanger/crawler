from datetime import datetime

from crawler.constants import CENTRE_KEY_NAME, FIELD_MONGODB_ID
from crawler.helpers.db_helpers import create_mongo_import_record
from crawler.helpers.logging_helpers import LoggingCollection


def test_create_mongo_import_record(freezer, mongo_database):
    config, mongo_database = mongo_database
    import_collection = mongo_database["imports"]

    docs = [{"x": 1}, {"y": 2}, {"z": 3}]
    error_collection = LoggingCollection()
    error_collection.add_error("TYPE 4", "error1")
    error_collection.add_error("TYPE 5", "error2")

    for centre in config.CENTRES:
        now = datetime.utcnow()
        result = create_mongo_import_record(
            import_collection, centre, len(docs), "test", error_collection.get_messages_for_import()
        )
        import_doc = import_collection.find_one({FIELD_MONGODB_ID: result.inserted_id})

        assert import_doc["date"].replace(microsecond=0) == now.replace(microsecond=0)
        assert import_doc["centre_name"] == centre[CENTRE_KEY_NAME]
        assert import_doc["csv_file_used"] == "test"
        assert import_doc["number_of_records"] == len(docs)
        assert import_doc["errors"] == error_collection.get_messages_for_import()
