import logging
from datetime import datetime
from typing import Any, List, Mapping

import pymongo
from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult

from crawler.constants import (
    CENTRE_KEY_NAME,
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    FIELD_BARCODE,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGO_LAB_ID,
    FIELD_MONGO_RESULT,
    FIELD_MONGO_RNA_ID,
    FIELD_MONGO_ROOT_SAMPLE_ID,
    FIELD_PLATE_BARCODE,
)
from crawler.db.mongo import get_mongo_collection
from crawler.types import CentreConf

logger = logging.getLogger(__name__)


def ensure_mongo_collections_indexed(database: Database) -> None:
    """Create required indexes on source_plates and samples collections in MongoDB.

    Arguments:
        database {Database}: the MongoDB database to create indexes on.
    """
    # get or create the source plates collection
    source_plates_collection = get_mongo_collection(database, COLLECTION_SOURCE_PLATES)

    logger.debug(f"Creating index '{FIELD_BARCODE}' on '{source_plates_collection.full_name}'")
    source_plates_collection.create_index(FIELD_BARCODE, unique=True)

    logger.debug(f"Creating index '{FIELD_LH_SOURCE_PLATE_UUID}' on '{source_plates_collection.full_name}'")
    source_plates_collection.create_index(FIELD_LH_SOURCE_PLATE_UUID, unique=True)

    samples_collection = get_mongo_collection(database, COLLECTION_SAMPLES)

    # Index on plate barcode to make it easier to select based on plate barcode
    logger.debug(f"Creating index '{FIELD_PLATE_BARCODE}' on '{samples_collection.full_name}'")
    samples_collection.create_index(FIELD_PLATE_BARCODE)

    # Index on result column to make it easier to select the positives
    logger.debug(f"Creating index '{FIELD_MONGO_RESULT}' on '{samples_collection.full_name}'")
    samples_collection.create_index(FIELD_MONGO_RESULT)

    # Index on lh_sample_uuid column to make it easier for queries joining on the samples from lighthouse
    # Index is sparse because not all rows have an lh_sample_uuid
    logger.debug(f"Creating index '{FIELD_LH_SAMPLE_UUID}' on '{samples_collection.full_name}'")
    samples_collection.create_index(FIELD_LH_SAMPLE_UUID, sparse=True)

    # Index on unique combination of columns
    logger.debug(f"Creating compound index on '{samples_collection.full_name}'")
    # create compound index on 'Root Sample ID', 'RNA ID', 'Result', 'Lab ID' - some
    # data had the same plate tested at another time so ignore the data if it is exactly
    # the same
    samples_collection.create_index(
        [
            (FIELD_MONGO_ROOT_SAMPLE_ID, pymongo.ASCENDING),
            (FIELD_MONGO_RNA_ID, pymongo.ASCENDING),
            (FIELD_MONGO_RESULT, pymongo.ASCENDING),
            (FIELD_MONGO_LAB_ID, pymongo.ASCENDING),
        ],
        unique=True,
    )

    # Index on lh_source_plate_uuid column
    # Added to make lighthouse API source completion event call query more efficient
    logger.debug(f"Creating index '{FIELD_LH_SOURCE_PLATE_UUID}' on '{samples_collection.full_name}'")
    samples_collection.create_index(FIELD_LH_SOURCE_PLATE_UUID)


def create_mongo_import_record(
    import_collection: Collection,
    centre: CentreConf,
    docs_inserted: int,
    file_name: str,
    errors: List[str],
) -> InsertOneResult:
    """Creates and inserts an import record for a centre.

    Arguments:
        import_collection {Collection}: the collection which stores import status documents
        centre {CentreConf}: the centre for which to store the import status
        docs_inserted {int}: to number of documents inserted for this centre
        file_name {str}: file parsed for samples
        errors {List[str]}: a list of errors while trying to process this centre

    Returns:
        InsertOneResult: the result of inserting this document
    """
    logger.debug(f"Creating the import record for {centre[CENTRE_KEY_NAME]}")

    import_doc = {
        "date": datetime.utcnow(),  # https://pymongo.readthedocs.io/en/stable/examples/datetimes.html
        "centre_name": centre[CENTRE_KEY_NAME],
        "csv_file_used": file_name,
        "number_of_records": docs_inserted,
        "errors": errors,
    }

    return import_collection.insert_one(document=import_doc)


def populate_mongo_collection(collection: Collection, documents: List[Mapping[str, Any]], filter_field: str) -> None:
    """Populates a collection using the given documents. It uses the filter_field to replace any documents that match
    the filter and adds any new documents.

    Arguments:
        collection {Collection}: collection to populate
        documents {List[Dict[str, Any]]}: documents to populate the collection with
        filter_field {str}: filter to search for matching documents
    """
    logger.debug(f"Populating/updating '{collection.full_name}' using '{filter_field}' as the filter")
    for document in documents:
        _ = collection.find_one_and_update(
            filter={filter_field: document[filter_field]}, update={"$set": document}, upsert=True
        )


def samples_filtered_for_duplicates_in_mongo(
    samples_collection: Collection, samples: List[Mapping[str, Any]], session: ClientSession = None
) -> List[Mapping[str, Any]]:
    dup_query = {
        "$or": [
            {
                FIELD_MONGO_LAB_ID: sample[FIELD_MONGO_LAB_ID],
                FIELD_MONGO_ROOT_SAMPLE_ID: sample[FIELD_MONGO_ROOT_SAMPLE_ID],
                FIELD_MONGO_RNA_ID: sample[FIELD_MONGO_RNA_ID],
                FIELD_MONGO_RESULT: sample[FIELD_MONGO_RESULT],
            }
            for sample in samples
        ]
    }

    result = samples_collection.find(dup_query, session=session)

    return [
        sample
        for dup_sample in result
        for sample in samples
        if sample[FIELD_MONGO_LAB_ID] == dup_sample[FIELD_MONGO_LAB_ID]
        and sample[FIELD_MONGO_ROOT_SAMPLE_ID] == dup_sample[FIELD_MONGO_ROOT_SAMPLE_ID]
        and sample[FIELD_MONGO_RNA_ID] == dup_sample[FIELD_MONGO_RNA_ID]
        and sample[FIELD_MONGO_RESULT] == dup_sample[FIELD_MONGO_RESULT]
    ]
