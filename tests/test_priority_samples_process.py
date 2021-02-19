from unittest.mock import patch
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
    MLWH_MUST_SEQUENCE,
    DART_STATE_PENDING,
    FIELD_COORDINATE,
)
import pytest
class TestStepTwo:
    @pytest.fixture(autouse=True)
    def mock_dart_calls(self):
        with patch("crawler.priority_samples_process.create_dart_sql_server_conn") as self.mock_conn:
            with patch("crawler.db.dart.get_dart_well_index") as self.mock_get_well_index:
                with patch("crawler.priority_samples_process.add_dart_plate_if_doesnt_exist") as self.mock_add_dart_plate:
                    with patch("crawler.db.dart.set_dart_well_properties") as self.mock_set_well_props:
                        with patch("crawler.db.dart.map_mongo_doc_to_dart_well_props") as self.mock_map:
                            test_well_props = {"prop1": "value1", "test prop": "test value"}
                            test_well_index = 15

                            self.mock_map.return_value = test_well_props
                            self.mock_get_well_index.return_value = test_well_index

                            yield



def test_step_two(config, mongo_database, mlwh_connection, testing_samples, testing_priority_samples):
    _, mongo_database = mongo_database

    #self.mock_add_dart_plate.side_effect = plates_status

    num_plates = 2
    num_wells = 3

    plates_status = [DART_STATE_PENDING, DART_STATE_PENDING]
    samples_root_sample_ids = ['MCM001','MCM002','MCM003','MCM004']
    priority_samples_root_sample_ids = ['MCM001','MCM002','MCM003','MCM004']
    priority_samples_processed = [True, True, False, True]

    with patch("crawler.priority_samples_process.create_dart_sql_server_conn") as mock_conn:
        with patch("crawler.db.dart.get_dart_well_index") as mock_get_well_index:
            with patch("crawler.priority_samples_process.add_dart_plate_if_doesnt_exist") as mock_add_dart_plate:
                with patch("crawler.db.dart.set_dart_well_properties") as mock_set_well_props:
                    with patch("crawler.db.dart.map_mongo_doc_to_dart_well_props") as mock_map:
                        test_well_props = {"prop1": "value1", "test prop": "test value"}
                        test_well_index = 15

                        mock_map.return_value = test_well_props
                        mock_get_well_index.return_value = test_well_index
                        mock_add_dart_plate.side_effect = plates_status

                        step_two(mongo_database, config)

                        # 2 plates created
                        assert mock_add_dart_plate.call_count == 2
                        # 3 wells checked in dart (2+1)
                        assert mock_get_well_index.call_count == 3
                        # 3 wells mapped to dart
                        assert mock_map.call_count == 3
                        # Only important samples
                        important_docs = [testing_samples[0], testing_samples[1], testing_samples[3]]
                        for doc in important_docs:
                            mock_get_well_index.assert_any_call(doc[FIELD_COORDINATE])

                        # 3 wells created in dart
                        assert mock_set_well_props.call_count == 3

                        # Wells created from first plate
                        mock_set_well_props.assert_any_call(
                            mock_conn().cursor(), "123", test_well_props, test_well_index
                        )
                        # Wells created from second plate
                        mock_set_well_props.assert_any_call(
                            mock_conn().cursor(), "456", test_well_props, test_well_index
                        )


                        # commits changes
                        mock_conn().cursor().rollback.assert_not_called()
                        # 1 commit/plate = 2 commits
                        assert mock_conn().cursor().commit.call_count == 2
                        mock_conn().close.assert_called_once()

    cursor = mlwh_connection.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME}")
    rows = cursor.fetchall()
    cursor.close()

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




# We have priority samples that have not been received yet (not in mongodb)
# Assert: dont do anything with them
def test_step_two_unprocessed_priority_sample_not_received_yet():
    assert False

# We have priority samples that were received a while ago
# Assert: process them
def test_step_two_unprocessed_priority_sample_already_received():
    assert False

# We have priority samples that were already processed
# Assert: dont do anything with them
def test_step_two_priority_sample_already_processed():
    assert False

# We have priority samples received and where the plate has already started in dart
# Assert: update all unpicked with priority samples changes
# Assert: it does not change status of picked samples
def test_step_two_priority_samples_received_dart_started():
    assert False

# We have priority samples received and where the plate has not started in dart
# Assert: update all unpicked with priority samples changes
def test_step_two_priority_samples_received_dart_pending():
    assert False

# We have priority samples received and where the plate has been completed in dart
# Assert: dont do anything
def test_step_two_priority_samples_received_dart_complete():
    assert False

