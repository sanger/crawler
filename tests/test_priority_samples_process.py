from unittest.mock import patch
from crawler.db.mongo import get_mongo_collection
from crawler.priority_samples_process import merge_priority_samples_into_docs_to_insert, step_two, logging_collection
from crawler.types import SampleDoc, SamplePriorityDoc, SampleDocValue
from typing import Union, List, Any
from crawler.constants import (
    FIELD_ROOT_SAMPLE_ID,
    FIELD_MUST_SEQUENCE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    COLLECTION_PRIORITY_SAMPLES,
    COLLECTION_SAMPLES,
    MLWH_TABLE_NAME,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_PREFERENTIALLY_SEQUENCE,
    MLWH_MUST_SEQUENCE,
    DART_STATE_PENDING,
    FIELD_COORDINATE,
    FIELD_PROCESSED,
    FIELD_PLATE_BARCODE,
)
import pytest

RecordScenarioList = Any

class TestStepTwo:
    @pytest.fixture(autouse=True)
    def mock_dart_calls(self, testing_samples, testing_priority_samples):
        with patch("crawler.priority_samples_process.create_dart_sql_server_conn") as self.mock_conn:
            with patch("crawler.db.dart.get_dart_well_index") as self.mock_get_well_index:
                with patch(
                    "crawler.priority_samples_process.add_dart_plate_if_doesnt_exist"
                ) as self.mock_add_dart_plate:
                    with patch("crawler.db.dart.set_dart_well_properties") as self.mock_set_well_props:
                        with patch("crawler.db.dart.map_mongo_doc_to_dart_well_props") as self.mock_map:
                            self.test_well_props = {"prop1": "value1", "test prop": "test value"}
                            self.test_well_index = 15

                            self.mock_map.return_value = self.test_well_props
                            self.mock_get_well_index.return_value = self.test_well_index
                            self.mock_add_dart_plate.side_effect = [DART_STATE_PENDING, DART_STATE_PENDING]

                            yield

    def set_property_values_for_collection(self, mongo_database, collection_name, elements, prop, values):
        collection = get_mongo_collection(mongo_database, collection_name)
        for i in range(0, len(values)):
            collection.find_one_and_update({"_id": elements[i]["_id"]}, {"$set": {prop: values[i]}})

    @pytest.fixture(
        params=[
            # Nothing to process "with_different_scenarios0"
            {
                "processed_status": [True, True, True, True],
                "plate_barcodes": ["123", "123", "123", "123"],
                "plates_status": {"123": DART_STATE_PENDING, "456": DART_STATE_PENDING},
                "expected_mlwh_samples": [False, False, False, False],
                "expected_samples": [],
                "expected_plates": [],
            },
            # Process all, one plate pending
            {
                "processed_status": [False, False, False, False],
                "plate_barcodes": ["123", "123", "123", "123"],
                "plates_status": {"123": DART_STATE_PENDING, "456": DART_STATE_PENDING},
                "expected_mlwh_samples": [True, True, True, True],
                "expected_samples": [True, True, True, True],
                "expected_plates": ["123"],
            },
            # Process all, different plates pending
            {
                "processed_status": [False, False, False, False],
                "plate_barcodes": ["123", "123", "456", "456"],
                "plates_status": {"123": DART_STATE_PENDING, "456": DART_STATE_PENDING},
                "expected_mlwh_samples": [True, True, True, True],
                "expected_samples": [True, True, True, True],
                "expected_plates": ["123", "456"],
            },
            # Process some, different plates pending
            {
                "processed_status": [False, True, False, True],
                "plate_barcodes": ["123", "123", "456", "456"],
                "plates_status": {"123": DART_STATE_PENDING, "456": DART_STATE_PENDING},
                "expected_mlwh_samples": [True, False, True, False],
                "expected_samples": [True, False, True, False],
                "expected_plates": ["123", "456"],
            },
            # Process some, different plates pending, one pending plate nothing to process
            {
                "processed_status": [False, True, True, True],
                "plate_barcodes": ["123", "123", "456", "456"],
                "plates_status": {"123": DART_STATE_PENDING, "456": DART_STATE_PENDING},
                "expected_mlwh_samples": [True, False, False, False],
                "expected_samples": [True, False, False, False],
                "expected_plates": ["123"],
            },
            # Process all, different plates, one plate running
            {
                "processed_status": [False, False, False, False],
                "plate_barcodes": ["123", "123", "456", "456"],
                "plates_status": {"123": DART_STATE_PENDING, "456": "RUNNING"},
                "expected_mlwh_samples": [True, True, True, True],
                "expected_samples": [True, True, False, False],
                "expected_plates": ["123", "456"],
            },
            # Process some, different plates, one plate running, one plate running nothing to process
            {
                "processed_status": [False, False, False, True],
                "plate_barcodes": ["123", "123", "123", "456"],
                "plates_status": {"123": DART_STATE_PENDING, "456": "RUNNING"},
                "expected_mlwh_samples": [True, True, True, False],
                "expected_samples": [True, True, True, False],
                "expected_plates": ["123"],
            },
            # Process all, different plates, all plates running, so no process
            {
                "processed_status": [False, False, False, False],
                "plate_barcodes": ["123", "123", "123", "456"],
                "plates_status": {"123": "RUNNING", "456": "RUNNING"},
                "expected_mlwh_samples": [True, True, True, True],
                "expected_samples": [False, False, False, False],
                "expected_plates": ["123", "456"],
            },
        ]
    )
    def with_different_scenarios(self, request, mongo_database, testing_samples, testing_priority_samples):
        _, mongo_database = mongo_database

        # Set processed samples
        self.processed_status = request.param["processed_status"]
        self.set_property_values_for_collection(
            mongo_database,
            COLLECTION_PRIORITY_SAMPLES,
            testing_priority_samples,
            FIELD_PROCESSED,
            self.processed_status,
        )

        # Set plate barcodes for each sample
        self.plate_barcodes = request.param["plate_barcodes"]
        self.set_property_values_for_collection(
            mongo_database, COLLECTION_SAMPLES, testing_samples, FIELD_PLATE_BARCODE, self.plate_barcodes
        )

        # Set plate status for each plate and mock calls
        self.plate_status = request.param["plates_status"]

        def find_plate_status(cursor, plate_barcode, lab_type):
            return self.plate_status[plate_barcode]

        self.mock_add_dart_plate.side_effect = find_plate_status

        def extract_mongo_record(info: RecordScenarioList) -> SampleDocValue:
            return info[0]

        # Set expected mlwh samples
        self.expected_mlwh_samples = list(
            map(
                extract_mongo_record,
                filter(lambda info: info[1], zip(testing_priority_samples, request.param["expected_mlwh_samples"])),
            )
        )

        # Set expected dart samples
        self.expected_samples = list(
            map(
                extract_mongo_record,
                filter(lambda info: info[1], zip(testing_samples, request.param["expected_samples"])),
            )
        )

        # Set expected dart plates
        self.expected_plates = request.param["expected_plates"]

    def validate_expected_data(self, with_different_scenarios, testing_priority_samples, testing_samples):
        # Check list of expected mlwh samples
        # Get unprocessed samples from list
        def extract_priority_sample(info: RecordScenarioList) -> SampleDocValue:
            return info[0]

        expected_mlwh_samples = list(
            map(
                extract_priority_sample,
                filter(lambda info: not (info[1]), zip(testing_priority_samples, self.processed_status)),
            )
        )
        assert self.expected_mlwh_samples == expected_mlwh_samples
        # Check list of expected dart samples and plates
        result = list(
            filter(
                lambda info: ((self.plate_status[info[1]] == DART_STATE_PENDING) and not (info[2])),
                zip(testing_samples, self.plate_barcodes, self.processed_status),
            )
        )
        if len(result) == 0:
            assert self.expected_samples == []
            assert sorted(self.expected_plates) == sorted(set(self.plate_barcodes))
        else:
            expected_samples, expected_plates, _ = zip(*result)
            expected_plates = set(expected_plates)
            assert self.expected_samples == list(expected_samples)
            assert self.expected_plates == list(expected_plates)

    def test_mlwh_was_correctly_updated_in_step_two(
        self, mongo_database, config, mlwh_connection, with_different_scenarios
    ):
        _, mongo_database = mongo_database
        step_two(mongo_database, config)
        cursor = mlwh_connection.cursor(dictionary=True)
        if len(self.expected_mlwh_samples) == 0:
            cursor.execute(f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME} ")
            rows = cursor.fetchall()
            cursor.close()
            assert len(rows) == 0
        else:
            root_sample_ids = ",".join(map(lambda x: f'"{x[FIELD_ROOT_SAMPLE_ID]}"', self.expected_mlwh_samples))
            cursor.execute(
                f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME} "
                f" WHERE {MLWH_ROOT_SAMPLE_ID} IN ({root_sample_ids})"
            )
            rows = cursor.fetchall()
            cursor.close()
            for pos, priority_sample in enumerate(self.expected_mlwh_samples):
                assert rows[pos][MLWH_ROOT_SAMPLE_ID] == priority_sample[FIELD_ROOT_SAMPLE_ID]
                assert rows[pos][MLWH_MUST_SEQUENCE] == priority_sample[FIELD_MUST_SEQUENCE]
                assert rows[pos][MLWH_PREFERENTIALLY_SEQUENCE] == priority_sample[FIELD_PREFERENTIALLY_SEQUENCE]

    def test_mlwh_insert_fails_in_step_two(self, config, mongo_database):
        _, mongo_database = mongo_database

        with patch("crawler.priority_samples_process.run_mysql_executemany_query", side_effect=Exception("Boom!")):
            step_two(mongo_database, config)

            assert logging_collection.get_count_of_all_errors_and_criticals() >= 1
            assert logging_collection.aggregator_types["TYPE 28"].count_errors == 1

    def test_mlwh_mysql_cannot_connect(self, config, mongo_database):
        _, mongo_database = mongo_database

        with patch("crawler.priority_samples_process.create_mysql_connection") as mock_connection:
            mock_connection().is_connected.return_value = False
            step_two(mongo_database, config)

            assert logging_collection.get_count_of_all_errors_and_criticals() >= 1
            assert logging_collection.aggregator_types["TYPE 29"].count_errors == 1

    def test_creates_right_number_of_plates_in_dart(self, mongo_database, config, with_different_scenarios):
        _, mongo_database = mongo_database

        step_two(mongo_database, config)

        # plates created
        assert self.mock_add_dart_plate.call_count == len(self.expected_plates)
        # 1 commit/plate = 2 commits
        assert self.mock_conn().cursor().commit.call_count == len(self.expected_plates)

    def test_creates_right_number_of_wells_in_dart(
        self, mongo_database, config, testing_samples, with_different_scenarios
    ):
        _, mongo_database = mongo_database

        num_wells = len(self.expected_samples)

        step_two(mongo_database, config)

        # wells checked in dart
        assert self.mock_get_well_index.call_count == num_wells
        # wells mapped to dart
        assert self.mock_map.call_count == num_wells

        for doc in self.expected_samples:
            self.mock_get_well_index.assert_any_call(doc[FIELD_COORDINATE])

        # wells created in dart
        assert self.mock_set_well_props.call_count == num_wells

        # Wells created from plate
        if num_wells > 0:
            for barcode in self.expected_plates:
                if self.plate_status[barcode] == DART_STATE_PENDING:
                    self.mock_set_well_props.assert_any_call(
                        self.mock_conn().cursor(), barcode, self.test_well_props, self.test_well_index
                    )

    def test_commits_changes_to_dart(self, mongo_database, config, with_different_scenarios):
        _, mongo_database = mongo_database

        step_two(mongo_database, config)

        # commits changes
        self.mock_conn().cursor().rollback.assert_not_called()

        # 1 commit per pending plate
        assert self.mock_conn().cursor().commit.call_count == len(self.expected_plates)
        self.mock_conn().close.assert_called_once()

    def test_adding_plate_and_wells_to_dart_fails_with_expection(self, mongo_database, config):
        _, mongo_database = mongo_database

        with patch("crawler.priority_samples_process.add_dart_well_properties", side_effect=Exception("Boom!")):
            step_two(mongo_database, config)

            assert logging_collection.get_count_of_all_errors_and_criticals() >= 1
            assert logging_collection.aggregator_types["TYPE 33"].count_errors == 1

    def test_adding_plate_and_wells_insert_failed(self, mongo_database, config):
        _, mongo_database = mongo_database

        with patch("crawler.priority_samples_process.create_dart_sql_server_conn") as mocked_conn:
            mocked_conn().cursor.side_effect = Exception("Boom!!")
            step_two(mongo_database, config)

            assert logging_collection.get_count_of_all_errors_and_criticals() >= 1
            assert logging_collection.aggregator_types["TYPE 30"].count_errors == 1

    def test_dart_sql_server_cannot_connect(self, config, mongo_database):
        _, mongo_database = mongo_database

        with patch("crawler.priority_samples_process.create_dart_sql_server_conn") as mock_conn:
            mock_conn.return_value = None

            step_two(mongo_database, config)

            assert logging_collection.get_count_of_all_errors_and_criticals() >= 1
            assert logging_collection.aggregator_types["TYPE 31"].count_errors == 1


# TODO remove testing_priority_samples from all test headers
def test_merge_priority_samples_into_docs_to_insert(
    mongo_database, config, testing_priority_samples, testing_docs_to_insert_for_aldp
):
    _, mongo_database = mongo_database

    centre_config = config.CENTRES[0]
    centre_config["sftp_root_read"] = "tmp/files"

    priority_samples_collection = get_mongo_collection(mongo_database, COLLECTION_PRIORITY_SAMPLES)
    root_sample_ids = ["MCM001", "MCM002"]
    priority_samples = list(priority_samples_collection.find({FIELD_ROOT_SAMPLE_ID: {"$in": root_sample_ids}}))

    merge_priority_samples_into_docs_to_insert(priority_samples, testing_docs_to_insert_for_aldp)

    assert (FIELD_MUST_SEQUENCE in testing_docs_to_insert_for_aldp[0]) is True
    assert (FIELD_MUST_SEQUENCE in testing_docs_to_insert_for_aldp[1]) is True
    assert (FIELD_PREFERENTIALLY_SEQUENCE in testing_docs_to_insert_for_aldp[0]) is True
    assert (FIELD_PREFERENTIALLY_SEQUENCE in testing_docs_to_insert_for_aldp[1]) is True


# We have priority samples that have not been received yet (not in mongodb)
# Assert: dont do anything with them
# def test_step_two_unprocessed_priority_sample_not_received_yet():

#     #assert False

# # We have priority samples that were received a while ago
# # Assert: process them
# def test_step_two_unprocessed_priority_sample_already_received():
#     #assert False

# # We have priority samples that were already processed
# # Assert: dont do anything with them
# def test_step_two_priority_sample_already_processed():
#     #assert False

# # We have priority samples received and where the plate has already started in dart
# # Assert: update all unpicked with priority samples changes
# # Assert: it does not change status of picked samples
# def test_step_two_priority_samples_received_dart_started():
#     #assert False

# # We have priority samples received and where the plate has not started in dart
# # Assert: update all unpicked with priority samples changes
# def test_step_two_priority_samples_received_dart_pending():
#     #assert False

# # We have priority samples received and where the plate has been completed in dart
# # Assert: dont do anything
# def test_step_two_priority_samples_received_dart_complete():
#     #assert False
