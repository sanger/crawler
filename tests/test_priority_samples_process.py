from unittest.mock import patch
from crawler.db.mongo import get_mongo_collection
from crawler.priority_samples_process import update_priority_samples, centre_config_for_samples, logging_collection
from typing import Dict, Tuple
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
    MLWH_MONGODB_ID,
    DART_STATE_PENDING,
    FIELD_COORDINATE,
    FIELD_PROCESSED,
    FIELD_PLATE_BARCODE,
    FIELD_SOURCE,
    FIELD_MONGODB_ID,
    FIELD_SAMPLE_ID,
)
import pytest
from bson.objectid import ObjectId


class TestPrioritySamplesProcess:
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
                "expected_dart_samples": [],
                "expected_dart_plates": [],
            },
            # Process all, one plate pending
            {
                "processed_status": [False, False, False, False],
                "plate_barcodes": ["123", "123", "123", "123"],
                "plates_status": {"123": DART_STATE_PENDING, "456": DART_STATE_PENDING},
                "expected_mlwh_samples": [True, True, True, True],
                "expected_dart_samples": [True, True, True, True],
                "expected_dart_plates": ["123"],
            },
            # Process all, different plates pending
            {
                "processed_status": [False, False, False, False],
                "plate_barcodes": ["123", "123", "456", "456"],
                "plates_status": {"123": DART_STATE_PENDING, "456": DART_STATE_PENDING},
                "expected_mlwh_samples": [True, True, True, True],
                "expected_dart_samples": [True, True, True, True],
                "expected_dart_plates": ["123", "456"],
            },
            # Process some, different plates pending
            {
                "processed_status": [False, True, False, True],
                "plate_barcodes": ["123", "123", "456", "456"],
                "plates_status": {"123": DART_STATE_PENDING, "456": DART_STATE_PENDING},
                "expected_mlwh_samples": [True, False, True, False],
                "expected_dart_samples": [True, False, True, False],
                "expected_dart_plates": ["123", "456"],
            },
            # Process some, different plates pending, one pending plate nothing to process
            {
                "processed_status": [False, True, True, True],
                "plate_barcodes": ["123", "123", "456", "456"],
                "plates_status": {"123": DART_STATE_PENDING, "456": DART_STATE_PENDING},
                "expected_mlwh_samples": [True, False, False, False],
                "expected_dart_samples": [True, False, False, False],
                "expected_dart_plates": ["123"],
            },
            # Process all, different plates, one plate running
            {
                "processed_status": [False, False, False, False],
                "plate_barcodes": ["123", "123", "456", "456"],
                "plates_status": {"123": DART_STATE_PENDING, "456": "RUNNING"},
                "expected_mlwh_samples": [True, True, True, True],
                "expected_dart_samples": [True, True, False, False],
                "expected_dart_plates": ["123", "456"],
            },
            # Process some, different plates, one plate running, one plate running nothing to process
            {
                "processed_status": [False, False, False, True],
                "plate_barcodes": ["123", "123", "123", "456"],
                "plates_status": {"123": DART_STATE_PENDING, "456": "RUNNING"},
                "expected_mlwh_samples": [True, True, True, False],
                "expected_dart_samples": [True, True, True, False],
                "expected_dart_plates": ["123"],
            },
            # Process all, different plates, all plates running, so no process
            {
                "processed_status": [False, False, False, False],
                "plate_barcodes": ["123", "123", "123", "456"],
                "plates_status": {"123": "RUNNING", "456": "RUNNING"},
                "expected_mlwh_samples": [True, True, True, True],
                "expected_dart_samples": [False, False, False, False],
                "expected_dart_plates": ["123", "456"],
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

        def extract_mongo_record(info: Tuple[Dict, bool]) -> Dict:
            return info[0]

        # Set expected mlwh samples
        self.expected_mlwh_samples = list(
            map(
                extract_mongo_record,
                filter(lambda info: info[1], zip(testing_priority_samples, request.param["expected_mlwh_samples"])),
            )
        )

        # Set expected dart samples
        self.expected_dart_samples = list(
            map(
                extract_mongo_record,
                filter(lambda info: info[1], zip(testing_samples, request.param["expected_dart_samples"])),
            )
        )

        # Set expected dart plates
        self.expected_dart_plates = request.param["expected_dart_plates"]


    def test_when_for_one_priority_sample_doesnt_exist_the_related_sample(
        self, mongo_database, config, mlwh_connection, with_different_scenarios
    ):
        # Creates one error sample priority
        _, mongo_database = mongo_database
        collection = get_mongo_collection(mongo_database, COLLECTION_PRIORITY_SAMPLES)
        _id = collection.find({})[0]["_id"]
        collection.find_one_and_update({"_id": _id}, {"$set": {"sample_id": "aaaaaaaxxxaaaaaaaaaaaaa1"}})

        try:
            update_priority_samples(mongo_database, config, True)
        except Exception:
            # Testing the match in IMPORTANT_UNPROCESSED_SAMPLES_MONGO_QUERY
            # so if there isnt a match, an Exception isn't thrown but handled
            pytest.fail("Unexpected error ..")

    def test_mlwh_not_updated_when_no_priority_samples_in_update_priority_samples(
        self, mongo_database, config, mlwh_connection, with_different_scenarios
    ):
        _, mongo_database = mongo_database
        update_priority_samples(mongo_database, config, True)
        cursor = mlwh_connection.cursor(dictionary=True)
        if len(self.expected_mlwh_samples) == 0:
            cursor.execute(f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME} ")
            rows = cursor.fetchall()
            cursor.close()
            assert len(rows) == 0

    def test_mlwh_was_correctly_updated_in_update_priority_samples(
        self, mongo_database, config, mlwh_connection, with_different_scenarios
    ):
        _, mongo_database = mongo_database
        update_priority_samples(mongo_database, config, True)
        cursor = mlwh_connection.cursor(dictionary=True)
        samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)

        if len(self.expected_mlwh_samples) > 0:
            mongodb_ids = ",".join(map(lambda x: f'"{x[FIELD_MONGODB_ID]}"', self.expected_mlwh_samples))
            cursor.execute(
                f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME} "
                f" WHERE {MLWH_MONGODB_ID} IN ({mongodb_ids})"
            )
            rows = cursor.fetchall()
            cursor.close()
            for pos, priority_sample in enumerate(self.expected_mlwh_samples):
                expected_sample = samples_collection.find({FIELD_MONGODB_ID: priority_sample[FIELD_SAMPLE_ID]})[0]

                assert ObjectId(rows[pos][MLWH_MONGODB_ID]) == priority_sample[FIELD_MONGODB_ID]
                assert rows[pos][MLWH_ROOT_SAMPLE_ID] == expected_sample[FIELD_ROOT_SAMPLE_ID]
                assert rows[pos][MLWH_MUST_SEQUENCE] == priority_sample[FIELD_MUST_SEQUENCE]
                assert rows[pos][MLWH_PREFERENTIALLY_SEQUENCE] == priority_sample[FIELD_PREFERENTIALLY_SEQUENCE]

    def test_mlwh_insert_fails_in_update_priority_samples(self, config, mongo_database):
        _, mongo_database = mongo_database

        with patch("crawler.db.mysql.run_mysql_executemany_query", side_effect=Exception("Boom!")):
            update_priority_samples(mongo_database, config, True)

            assert logging_collection.get_count_of_all_errors_and_criticals() >= 1
            assert logging_collection.aggregator_types["TYPE 28"].count_errors == 1

    def test_mlwh_mysql_cannot_connect(self, config, mongo_database):
        _, mongo_database = mongo_database

        with patch("crawler.db.mysql.create_mysql_connection") as mock_connection:
            mock_connection().is_connected.return_value = False
            update_priority_samples(mongo_database, config, True)

            assert logging_collection.get_count_of_all_errors_and_criticals() >= 1
            assert logging_collection.aggregator_types["TYPE 29"].count_errors == 1

    def test_creates_right_number_of_plates_in_dart(self, mongo_database, config, with_different_scenarios):
        _, mongo_database = mongo_database

        update_priority_samples(mongo_database, config, True)

        # plates created
        assert self.mock_add_dart_plate.call_count == len(self.expected_dart_plates)
        # 1 commit/plate = 2 commits
        assert self.mock_conn().cursor().commit.call_count == len(self.expected_dart_plates)

    def test_creates_right_number_of_wells_in_dart(
        self, mongo_database, config, testing_samples, with_different_scenarios
    ):
        _, mongo_database = mongo_database

        num_wells = len(self.expected_dart_samples)

        update_priority_samples(mongo_database, config, True)

        # wells checked in dart
        assert self.mock_get_well_index.call_count == num_wells
        # wells mapped to dart
        assert self.mock_map.call_count == num_wells

        for doc in self.expected_dart_samples:
            self.mock_get_well_index.assert_any_call(doc[FIELD_COORDINATE])

        # wells created in dart
        assert self.mock_set_well_props.call_count == num_wells

        # Wells created from plate
        if num_wells > 0:
            for barcode in self.expected_dart_plates:
                if self.plate_status[barcode] == DART_STATE_PENDING:
                    self.mock_set_well_props.assert_any_call(
                        self.mock_conn().cursor(), barcode, self.test_well_props, self.test_well_index
                    )

    def test_commits_changes_to_dart(self, mongo_database, config, with_different_scenarios):
        _, mongo_database = mongo_database

        update_priority_samples(mongo_database, config, True)

        # commits changes
        self.mock_conn().cursor().rollback.assert_not_called()

        # 1 commit per pending plate
        assert self.mock_conn().cursor().commit.call_count == len(self.expected_dart_plates)
        self.mock_conn().close.assert_called_once()

    def test_adding_plate_and_wells_to_dart_fails_with_expection(self, mongo_database, config):
        _, mongo_database = mongo_database

        with patch("crawler.priority_samples_process.add_dart_well_properties", side_effect=Exception("Boom!")):
            update_priority_samples(mongo_database, config, True)

            assert logging_collection.get_count_of_all_errors_and_criticals() >= 1
            assert logging_collection.aggregator_types["TYPE 33"].count_errors == 1

    def test_adding_plate_and_wells_insert_failed(self, mongo_database, config):
        _, mongo_database = mongo_database

        with patch("crawler.priority_samples_process.create_dart_sql_server_conn") as mocked_conn:
            mocked_conn().cursor.side_effect = Exception("Boom!!")
            update_priority_samples(mongo_database, config, True)

            assert logging_collection.get_count_of_all_errors_and_criticals() >= 1
            assert logging_collection.aggregator_types["TYPE 30"].count_errors == 1

    def test_dart_sql_server_cannot_connect(self, config, mongo_database):
        _, mongo_database = mongo_database

        with patch("crawler.priority_samples_process.create_dart_sql_server_conn") as mock_conn:
            mock_conn.return_value = None

            update_priority_samples(mongo_database, config, True)

            assert logging_collection.get_count_of_all_errors_and_criticals() >= 1
            assert logging_collection.aggregator_types["TYPE 31"].count_errors == 1

    def test_centre_config_for_samples(self, config):
        result = centre_config_for_samples(config, [{FIELD_SOURCE: "Test Centre"}])
        assert result["name"] == "Test Centre"
        assert result["prefix"] == "TEST"
        assert result["lab_id_default"] == "TE"
