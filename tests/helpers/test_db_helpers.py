from datetime import datetime

from crawler.constants import (
    CENTRE_KEY_NAME,
    COLLECTION_CENTRES,
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    FIELD_BARCODE,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGO_LAB_ID,
    FIELD_MONGO_RESULT,
    FIELD_MONGO_RNA_ID,
    FIELD_MONGO_ROOT_SAMPLE_ID,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
)
from crawler.helpers.db_helpers import (
    create_mongo_import_record,
    ensure_mongo_collections_indexed,
    populate_mongo_collection,
)
from crawler.helpers.logging_helpers import LoggingCollection


def test_ensure_mongo_collections_indexed_adds_correct_indexes_to_source_plates(mongo_database):
    _, mongo_database = mongo_database

    ensure_mongo_collections_indexed(mongo_database)

    source_plates_collection = mongo_database[COLLECTION_SOURCE_PLATES]
    source_plates_indexes = source_plates_collection.index_information()
    assert len(source_plates_indexes) == 3  # Default _id index plus two we added.
    assert list(source_plates_indexes.keys()) == [
        "_id_",
        FIELD_BARCODE + "_1",
        FIELD_LH_SOURCE_PLATE_UUID + "_1",
    ]


def test_ensure_mongo_collections_indexed_adds_correct_indexes_to_samples(mongo_database):
    _, mongo_database = mongo_database

    ensure_mongo_collections_indexed(mongo_database)

    samples_collection = mongo_database[COLLECTION_SAMPLES]
    samples_indexes = samples_collection.index_information()
    assert len(samples_indexes) == 6  # Default _id index plus five we added.
    assert list(samples_indexes.keys()) == [
        "_id_",
        FIELD_PLATE_BARCODE + "_1",
        FIELD_MONGO_RESULT + "_1",
        FIELD_LH_SAMPLE_UUID + "_1",
        f"{FIELD_MONGO_ROOT_SAMPLE_ID}_1_{FIELD_MONGO_RNA_ID}_1_{FIELD_MONGO_RESULT}_1_{FIELD_MONGO_LAB_ID}_1",
        FIELD_LH_SOURCE_PLATE_UUID + "_1",
    ]


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


def test_populate_mongo_collection_inserts_documents(mongo_database):
    _, mongo_database = mongo_database
    centres_collection = mongo_database[COLLECTION_CENTRES]

    doc1 = {"Key": "A", "Data": "data1"}
    doc2 = {"Key": "B", "Data": "data2"}
    populate_mongo_collection(centres_collection, [doc1, doc2], "Key")

    assert centres_collection.count_documents({}) == 2
    assert centres_collection.count_documents(doc1) == 1
    assert centres_collection.count_documents(doc2) == 1


def test_populate_mongo_collection_upserts_documents(mongo_database):
    _, mongo_database = mongo_database
    centres_collection = mongo_database[COLLECTION_CENTRES]

    doc1 = {"Key": "A", "Data": "data1"}
    doc2 = {"Key": "B", "Data": "data2"}

    populate_mongo_collection(centres_collection, [doc1, doc2], "Key")

    replacement_doc1 = {"Key": "A", "Data": "data4"}
    doc3 = {"Key": "C", "Data": "data3"}

    populate_mongo_collection(centres_collection, [replacement_doc1, doc3], "Key")

    assert centres_collection.count_documents({}) == 3
    assert centres_collection.count_documents(doc1) == 0
    assert centres_collection.count_documents(doc2) == 1
    assert centres_collection.count_documents(replacement_doc1) == 1
    assert centres_collection.count_documents(doc3) == 1
