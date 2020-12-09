from types import ModuleType
from crawler.constants import (
    COLLECTION_SAMPLES,
)
from crawler.db import (
    create_mongo_client,
    get_mongo_collection,
    get_mongo_db,
)

def filtered_positive_fields_exist(config: ModuleType):
    """Determines whether filtered positive fields exist in database

    Arguments:
        None
    
    Returns:
        Boolean -- Filtered positive fields exist
    """
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        return list(samples_collection.find({
            "$or" : [
                { FIELD_FILTERED_POSITIVE : {"$exists": True} },
                { FIELD_FILTERED_POSITIVE_VERSION : {"$exists": True} },
                { FIELD_FILTERED_POSITIVE_TIMESTAMP : {"$exists": True} }
            ]
        }))


def all_mongo_samples(config: ModuleType):
    """Gets all samples from Mongo

    Arguments:
        None
    
    Returns:
        Boolean -- Filtered positive fields exist
    """
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        return list(samples_collection.find({}))

