from crawler.db.mongo import get_mongo_collection
from crawler.file_processing import ERRORS_DIR, SUCCESSES_DIR, Centre, CentreFile
from crawler.priority_samples_process import (
    merge_priority_samples_into_docs_to_insert,
    step_two
)

from crawler.constants import (
    FIELD_ROOT_SAMPLE_ID,
    FIELD_MUST_SEQUENCE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    COLLECTION_PRIORITY_SAMPLES,
    MLWH_TABLE_NAME,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_PREFERENTIALLY_SEQUENCE,
    MLWH_MUST_SEQUENCE
)

def test_step_two(config, mongo_database, mlwh_connection, testing_samples, testing_priority_samples):
    _, mongo_database = mongo_database

    # with patch("crawler.priority_samples_process.create_dart_sql_server_conn") as mock_conn:
    #     with patch("crawler.db.dart.get_dart_well_index") as mock_get_well_index:
    #         test_well_index = 15
    #         mock_get_well_index.return_value = test_well_index

    #         # calls for well index and to map as expected
    #         assert mock_get_well_index.call_count == 3
    #         assert mock_map.call_count == 3
    #         # Only important samples
    #         important_docs = [docs_to_insert[0], docs_to_insert[1], docs_to_insert[3]]
    #         for doc in important_docs:
    #             mock_get_well_index.assert_any_call(doc[FIELD_COORDINATE])
    #             mock_map.assert_any_call(doc)

    #         # commits changes
    #         mock_conn().cursor().rollback.assert_not_called()
    #         assert mock_conn().cursor().commit.call_count == 1
    #         mock_conn().close.assert_called_once()

    step_two(mongo_database, config)

    cursor = mlwh_connection.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME}")
    rows = cursor.fetchall()
    cursor.close()

    # import pdb
    # pdb.set_trace()

    assert rows[0][MLWH_ROOT_SAMPLE_ID] == 'MCM001'
    assert rows[0]['must_sequence'] == 1
    assert rows[0]['preferentially_sequence'] == 0
    assert rows[1][MLWH_ROOT_SAMPLE_ID] == 'MCM002'
    assert rows[1]['must_sequence'] == 0
    assert rows[1]['preferentially_sequence'] == 1
    assert rows[2][MLWH_ROOT_SAMPLE_ID] == 'MCM004'
    assert rows[2]['must_sequence'] == 0
    assert rows[2]['preferentially_sequence'] == 0


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
