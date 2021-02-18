from crawler.db.mongo import get_mongo_collection
from crawler.file_processing import ERRORS_DIR, SUCCESSES_DIR, Centre, CentreFile
from crawler.priority_samples_process import merge_priority_samples_into_docs_to_insert

from crawler.constants import (
    FIELD_ROOT_SAMPLE_ID,
    FIELD_MUST_SEQUENCE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    COLLECTION_PRIORITY_SAMPLES,
)

def test_merge_priority_samples_into_docs_to_insert(mongo_database, config, testing_priority_samples, testing_docs_to_insert_for_aldp):
    _, mongo_database = mongo_database

    centre_config = config.CENTRES[0]
    centre_config["sftp_root_read"] = "tmp/files"
    centre = Centre(config, centre_config)
    centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)

    priority_samples_collection = get_mongo_collection(mongo_database, COLLECTION_PRIORITY_SAMPLES)
    root_sample_ids = ["MCM001", "MCM002"]
    priority_samples = list(priority_samples_collection.find({FIELD_ROOT_SAMPLE_ID: {"$in": root_sample_ids}}))

    merge_priority_samples_into_docs_to_insert(priority_samples, testing_docs_to_insert_for_aldp)

    assert (FIELD_MUST_SEQUENCE in testing_docs_to_insert_for_aldp[0]) == True
    assert (FIELD_MUST_SEQUENCE in testing_docs_to_insert_for_aldp[1]) == True
    assert (FIELD_PREFERENTIALLY_SEQUENCE in testing_docs_to_insert_for_aldp[0]) == True
    assert (FIELD_PREFERENTIALLY_SEQUENCE in testing_docs_to_insert_for_aldp[1]) == True
