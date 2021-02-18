import logging
import logging.config

from typing import Any, List

from crawler.db.mongo import (
    get_mongo_collection,
)

from crawler.constants import (
    COLLECTION_PRIORITY_SAMPLES,
    COLLECTION_SAMPLES,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_MUST_SEQUENCE,
    FIELD_PREFERENTIALLY_SEQUENCE,
)

from crawler.db.mysql import (
    insert_samples_from_docs_into_mlwh
)


logger = logging.getLogger(__name__)


# priority_samples_root_ids = find on priority_samples where processed is false, and must_seq / pref_seq == true
# get priority_samples which exist in samples = get_samples_from_root_sample_ids(priority_sample_root_ids)

# merge priority data with samples
# update MLWH
# update DART
# set processed in priority_samples collection to true
# logger message, as cant log on a file
# posisble move to seperate script
def step_two(db) -> None:
    """
    Description
    Arguments:
        x {Type} -- description
    """

    logger.info(f"Starting Step 2")

    priority_samples_collection = get_mongo_collection(db, COLLECTION_PRIORITY_SAMPLES)
    unprocessed_priority_samples = get_all_unprocessed_priority_samples(db)
    unprocessed_priority_samples_root_sample_ids = list(map(lambda x: x[FIELD_ROOT_SAMPLE_ID], unprocessed_priority_samples))

    samples = get_samples_for_root_sample_ids(db, unprocessed_priority_samples_root_sample_ids)

    merge_priority_samples_into_docs_to_insert(unprocessed_priority_samples, samples)

    #  Create all samples in MLWH with docs_to_insert including must_seq/ pre_seq
    mlwh_success = insert_samples_from_docs_into_mlwh(samples)

    # add to the DART database if the config flag is set and we have successfully updated the MLWH
    if mlwh_success:
        logger.info("MLWH insert successful and adding to DART")

        #  Create in DART with docs_to_insert including must_seq/ pre_seq
        #  use docs_to_insert to update DART
        dart_success = insert_plates_and_wells_from_docs_into_dart(samples)
        if dart_success:
            # use stored identifiers to update priority_samples table to processed true
            priority_samples_root_samples_id = list(map(lambda x: x[FIELD_ROOT_SAMPLE_ID], unprocessed_priority_samples))
            update_priority_samples_to_processed(priority_samples_root_samples_id)


def merge_priority_samples_into_docs_to_insert(self, priority_samples: List[Any], docs_to_insert) -> None:
    """
    Updates the sample records with must_sequence and preferentially_sequence values

    for each successful add sample, merge into docs_to_insert_mlwh
    with must_sequence and preferentially_sequence values

    Arguments:
        priority_samples  - priority samples to update docs_to_insert with
        docs_to_insert {List[ModifiedRow]} -- the sample records to update
    """
    priority_root_sample_ids = list(map(lambda x: x[FIELD_ROOT_SAMPLE_ID], priority_samples))

    for doc in docs_to_insert:
        root_sample_id = doc[FIELD_ROOT_SAMPLE_ID]
        if root_sample_id in priority_root_sample_ids:
            priority_sample = list(filter(lambda x: x[FIELD_ROOT_SAMPLE_ID] == root_sample_id, priority_samples))[0]
            doc[FIELD_MUST_SEQUENCE] = priority_sample[FIELD_MUST_SEQUENCE]
            doc[FIELD_PREFERENTIALLY_SEQUENCE] = priority_sample[FIELD_PREFERENTIALLY_SEQUENCE]


def get_samples_for_root_sample_ids(db, root_sample_ids) -> List[Any]:
    """
    Description
    Arguments:
        x {Type} -- description
    """
    samples_collection = get_mongo_collection(db, COLLECTION_SAMPLES)
    return list(map(lambda x: x, samples_collection.find({FIELD_ROOT_SAMPLE_ID: {"$in": root_sample_ids}})))


def update_priority_samples_to_processed(db, root_sample_ids) -> bool:
    """
    Description
    use stored identifiers to update priority_samples table to processed true
    Arguments:
        x {Type} -- description
    """
    priority_samples_collection = get_mongo_collection(db, COLLECTION_PRIORITY_SAMPLES)
    for root_sample_id in root_sample_ids:
        priority_samples_collection.update({"Root Sample ID": root_sample_id}, {"$set": {"processed": True}})
    logger.info("Mongo update of processed for priority samples successful")


def get_unprocessed_priority_samples_for_root_sample_ids(self, db, root_sample_ids: List[str]) -> List[Any]:
    """
    Description
    check if sample is in priority_samples either must_sequence/preferentially_sequence is true, and processed false
    Arguments:
        x {Type} -- description
    """
    matching_priority_entry ={FIELD_ROOT_SAMPLE_ID: {"$in": root_sample_ids}}
    unprocessed = { "processed": False }
    of_importance = {"$or": [{"must_sequence": True}, {"preferentially_sequence": True}]}

    query = {"$and": [matching_priority_entry, unprocessed, of_importance]}

    priority_samples_collection = get_mongo_collection(db, COLLECTION_PRIORITY_SAMPLES)

    priority_sample_cursor = priority_samples_collection.find(query)
    return list(priority_sample_cursor)


def get_all_unprocessed_priority_samples(db) -> List[Any]:
    """
    Description
    Arguments:
        x {Type} -- description
    """
    unprocessed = { "processed": False }
    of_importance = {"$or": [{"must_sequence": True}, {"preferentially_sequence": True}]}

    query = {"$and": [unprocessed, of_importance]}

    priority_samples_collection = get_mongo_collection(db, COLLECTION_PRIORITY_SAMPLES)
    priority_sample_cursor = priority_samples_collection.find(query)
    return list(priority_sample_cursor)



