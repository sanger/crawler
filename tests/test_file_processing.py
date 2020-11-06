import logging
import logging.config
import shutil
import os
from io import StringIO
from crawler.helpers import current_time
from unittest.mock import (
  patch,
  MagicMock,
)
from csv import DictReader
import pytest
from datetime import (
    datetime,
    timezone,
)
from bson.objectid import ObjectId
from decimal import Decimal
from bson.decimal128 import Decimal128 # type: ignore
from tempfile import mkstemp
from crawler.file_processing import (
    Centre,
    CentreFile,
    CentreFileState,
    SUCCESSES_DIR,
    ERRORS_DIR
)
from crawler.constants import (
    COLLECTION_CENTRES,
    COLLECTION_IMPORTS,
    COLLECTION_SAMPLES,
    COLLECTION_SAMPLES_HISTORY,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_RNA_ID,
    FIELD_RESULT,
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
    FIELD_VIRAL_PREP_ID,
    FIELD_RNA_PCR_ID,
    FIELD_PLATE_BARCODE,
    FIELD_COORDINATE,
    FIELD_SOURCE,
    FIELD_CREATED_AT,
    FIELD_UPDATED_AT,
    FIELD_CH1_TARGET,
    FIELD_CH1_RESULT,
    FIELD_CH1_CQ,
    FIELD_CH2_TARGET,
    FIELD_CH2_RESULT,
    FIELD_CH2_CQ,
    FIELD_CH3_TARGET,
    FIELD_CH3_RESULT,
    FIELD_CH3_CQ,
    FIELD_CH4_TARGET,
    FIELD_CH4_RESULT,
    FIELD_CH4_CQ,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    MLWH_TABLE_NAME,
    MLWH_TABLE_NAME,
    MLWH_MONGODB_ID,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_RNA_ID,
    MLWH_PLATE_BARCODE,
    MLWH_COORDINATE,
    MLWH_RESULT,
    MLWH_DATE_TESTED_STRING,
    MLWH_DATE_TESTED,
    MLWH_SOURCE,
    MLWH_LAB_ID,
    MLWH_CH1_TARGET,
    MLWH_CH1_RESULT,
    MLWH_CH1_CQ,
    MLWH_CH2_TARGET,
    MLWH_CH2_RESULT,
    MLWH_CH2_CQ,
    MLWH_CH3_TARGET,
    MLWH_CH3_RESULT,
    MLWH_CH3_CQ,
    MLWH_CH4_TARGET,
    MLWH_CH4_RESULT,
    MLWH_CH4_CQ,
    MLWH_FILTERED_POSITIVE,
    MLWH_FILTERED_POSITIVE_VERSION,
    MLWH_FILTERED_POSITIVE_TIMESTAMP,
    MLWH_CREATED_AT,
    MLWH_UPDATED_AT,
    POSITIVE_RESULT_VALUE,
)
from crawler.db import get_mongo_collection


# ----- tests helpers -----

def centre_file_with_mocked_filtered_postitive_identifier(config, file_name):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile(file_name, centre)
    centre_file.filtered_positive_identifier.current_version = MagicMock(return_value = 'v2.3')
    centre_file.filtered_positive_identifier.is_positive = MagicMock(return_value = True)
    return centre_file

# ----- tests for class Centre -----

def test_get_download_dir(config):
    for centre_config in config.CENTRES:
        centre = Centre(config, centre_config)

        assert centre.get_download_dir() == f"{config.DIR_DOWNLOADED_DATA}{centre_config['prefix']}/"

def test_process_files(mongo_database, config, testing_files_for_process, testing_centres, pyodbc_conn):
    _, mongo_database = mongo_database
    logger = logging.getLogger(__name__)

    centre_config = config.CENTRES[0]
    centre_config["sftp_root_read"] = "tmp/files"
    centre = Centre(config, centre_config)
    centre.process_files()

    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    samples_history_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES_HISTORY)

    # # We record *all* our samples
    assert samples_collection.count_documents({"RNA ID": "123_B09", "source": "Alderley"}) == 1



# ----- tests for class CentreFile -----

# tests for checksums
def create_checksum_files_for(filepath, filename, checksums, timestamp):
    list_files = []
    for checksum in checksums:
        full_filename = f"{filepath}/{timestamp}_{filename}_{checksum}"
        file = open(full_filename, "w")
        file.write("Your text goes here")
        file.close()
        list_files.append(full_filename)
    return list_files

def test_checksum_not_match(config, tmpdir):
    with patch.dict(config.CENTRES[0], {"backups_folder": tmpdir.realpath()}):
        tmpdir.mkdir("successes")

        list_files = create_checksum_files_for(
            f"{config.CENTRES[0]['backups_folder']}/successes/",
            "AP_sanger_report_200503_2338.csv",
            ["adfsadf", "asdf"],
            "200601_1414",
        )

        try:
            centre = Centre(config, config.CENTRES[0])
            centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)

            assert centre_file.checksum_match("successes") == False
        finally:
            for tmpfile_for_list in list_files:
                os.remove(tmpfile_for_list)

def test_checksum_match(config, tmpdir):
    with patch.dict(config.CENTRES[0], {"backups_folder": tmpdir.realpath()}):

        tmpdir.mkdir("successes")

        list_files = create_checksum_files_for(
            f"{config.CENTRES[0]['backups_folder']}/successes/",
            "AP_sanger_report_200503_2338.csv",
            ["adfsadf", "5c11524df6fd623ae3d687d66152be28"],
            "200601_1414",
        )

        try:
            centre = Centre(config, config.CENTRES[0])
            centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)
            assert centre_file.checksum_match("successes") == True
        finally:
            for tmpfile_for_list in list_files:
                os.remove(tmpfile_for_list)

# tests for validating row structure
def test_row_required_fields_present_fail(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    # Not maching regexp
    assert not centre_file.row_required_fields_present(
        {"Root Sample ID": "asdf", "Result": "Positive", "RNA ID": "", "Date tested": "adsf"}, 6
    ), "No RNA id"

    assert not centre_file.row_required_fields_present(
        {"Root Sample ID": "asdf", "Result": "", "RNA ID": "", "Date Tested": "date"}, 1
    ), "Not barcode"

    # All required but all empty
    assert not centre_file.row_required_fields_present(
        {"Root Sample ID": "", "Result": "", "RNA ID": "", "Date tested": ""}, 4
    ), "All are empty"

def test_row_required_fields_present(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    assert centre_file.row_required_fields_present(
        {"Root Sample ID": "asdf", "Result": "asdf", "RNA ID": "ASDF_A01", "Date tested": "asdf"}, 5
    )

    assert not (
        centre_file.row_required_fields_present(
            {"Root Sample ID": "asdf", "Result": "", "RNA ID": "ASDF_A01", "Date tested": ""}, 5
        )
    )

    assert not (
        centre_file.row_required_fields_present(
            {"Root Sample ID": "asdf", "Result": "Positive", "RNA ID": "", "Date tested": ""}, 5
        )
    )

# tests for extracting information from field values
def test_extract_plate_barcode_and_coordinate(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    barcode_field = "RNA ID"
    barcode_regex = r"^(.*)_([A-Z]\d\d)$"

    # typical format
    assert centre_file.extract_plate_barcode_and_coordinate(
        {"RNA ID": "AP-abc-12345678_H01"}, 0, barcode_field, barcode_regex
    ) == (
        "AP-abc-12345678",
        "H01",
    )

    # coordinate zero
    assert centre_file.extract_plate_barcode_and_coordinate(
        {"RNA ID": "AP-abc-12345678_A00"}, 0, barcode_field, barcode_regex
    ) == (
        "AP-abc-12345678",
        "A00",
    )

    # invalid coordinate format
    assert centre_file.extract_plate_barcode_and_coordinate(
        {"RNA ID": "AP-abc-12345678_H0"}, 0, barcode_field, barcode_regex
    ) == (
        "",
        "",
    )

    # missing underscore between plate barcode and coordinate
    assert centre_file.extract_plate_barcode_and_coordinate(
        {"RNA ID": "AP-abc-12345678H0"}, 0, barcode_field, barcode_regex
    ) == (
        "",
        "",
    )

    # shorter plate barcode
    assert centre_file.extract_plate_barcode_and_coordinate(
        {"RNA ID": "DN1234567_H01"}, 0, barcode_field, barcode_regex
    ) == (
        "DN1234567",
        "H01",
    )

# tests for parsing and formatting the csv file rows
def test_parse_and_format_file_rows(config):
    timestamp = "some timestamp"
    centre_file = centre_file_with_mocked_filtered_postitive_identifier(config, 'some file')
    with patch.object(centre_file, "get_now_timestamp", return_value=timestamp):
        extra_fields_added = [
            {
                "Root Sample ID": "1",
                "RNA ID": "RNA_0043_H09",
                "plate_barcode": "RNA_0043",
                "source": "Alderley",
                "coordinate": "H09",
                "line_number": 2,
                "Result": "Positive",
                "file_name": "some file",
                "file_name_date": None,
                "created_at": timestamp,
                "updated_at": timestamp,
                "Lab ID": None,
                "filtered_positive": True,
                "filtered_positive_version": 'v2.3',
                "filtered_positive_timestamp": timestamp,
            }
        ]

        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID\n")
            fake_csv.write("1,RNA_0043_H09,Positive\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)
            assert augmented_data == extra_fields_added
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0

        wrong_barcode = [
            {
                "Root Sample ID": "1",
                "RNA ID": "RNA_0043_",
                "Result": "",
                "plate_barcode": "",
                "source": "Alderley",
                "coordinate": "",
                "Lab ID": "",
            }
        ]

        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID\n")
            fake_csv.write("1,RNA_0043_,Positive\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)
            assert augmented_data == []

            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
            assert centre_file.logging_collection.aggregator_types["TYPE 9"].count_errors == 1

def test_filtered_row_with_extra_unrecognised_columns(config):
    # check have removed extra columns and created a warning error log
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with StringIO() as fake_csv_with_extra_columns:
        fake_csv_with_extra_columns.write(
            f"{FIELD_ROOT_SAMPLE_ID},{FIELD_RNA_ID},{FIELD_RESULT},{FIELD_DATE_TESTED},{FIELD_LAB_ID},{FIELD_CH1_TARGET},{FIELD_CH1_RESULT},{FIELD_CH1_CQ},extra_col_1,extra_col_2,extra_col_3\n"
        )
        fake_csv_with_extra_columns.write(
            "1,RNA_0043,Positive,today,AP,ORF1ab,Positive,23.12345678,extra_value_1,extra_value_2,extra_value_3\n"
        )
        fake_csv_with_extra_columns.seek(0)

        csv_to_test_reader = DictReader(fake_csv_with_extra_columns)

        expected_row = {
            "Root Sample ID": "1",
            "RNA ID": "RNA_0043",
            "Result": "Positive",
            "Date Tested": "today",
            "Lab ID": "AP",
            "CH1-Target":"ORF1ab",
            "CH1-Result":"Positive",
            "CH1-Cq":"23.12345678",
        }

        assert centre_file.filtered_row(next(csv_to_test_reader), 2) == expected_row
        assert centre_file.logging_collection.aggregator_types["TYPE 13"].count_errors == 1
        # N.B. Type 13 is a WARNING type and not counted as an error or critical
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0

def test_filtered_row_with_blank_lab_id(config):
    # check when flag set in config it adds default lab id
    try:
        config.ADD_LAB_ID = True
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some_file.csv", centre)

        with StringIO() as fake_csv_without_lab_id:
            fake_csv_without_lab_id.write(
                f"{FIELD_ROOT_SAMPLE_ID},{FIELD_RNA_ID},{FIELD_RESULT},{FIELD_DATE_TESTED}\n"
            )
            fake_csv_without_lab_id.write("1,RNA_0043,Positive,today\n")
            fake_csv_without_lab_id.seek(0)

            csv_to_test_reader = DictReader(fake_csv_without_lab_id)

            expected_row = {
                "Root Sample ID": "1",
                "RNA ID": "RNA_0043",
                "Result": "Positive",
                "Date Tested": "today",
                "Lab ID": "AP",
            }

            assert centre_file.filtered_row(next(csv_to_test_reader), 2) == expected_row
            assert centre_file.logging_collection.aggregator_types["TYPE 12"].count_errors == 1
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
    finally:
        config.ADD_LAB_ID = False

def test_filtered_row_with_lab_id_present(config):
    # check when flag set in config it adds default lab id
    try:
        config.ADD_LAB_ID = True
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some_file.csv", centre)

        with StringIO() as fake_csv_without_lab_id:
            fake_csv_without_lab_id.write(
                f"{FIELD_ROOT_SAMPLE_ID},{FIELD_RNA_ID},{FIELD_RESULT},{FIELD_DATE_TESTED},{FIELD_LAB_ID}\n"
            )
            fake_csv_without_lab_id.write("1,RNA_0043,Positive,today,RealLabID\n")
            fake_csv_without_lab_id.seek(0)

            csv_to_test_reader = DictReader(fake_csv_without_lab_id)

            expected_row = {
                "Root Sample ID": "1",
                "RNA ID": "RNA_0043",
                "Result": "Positive",
                "Date Tested": "today",
                "Lab ID": "RealLabID",
            }

            assert centre_file.filtered_row(next(csv_to_test_reader), 2) == expected_row
            assert centre_file.logging_collection.aggregator_types["TYPE 12"].count_errors == 0
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0
    finally:
        config.ADD_LAB_ID = False

def test_filtered_row_with_ct_channel_columns(config):
    # check can handle a row with the channel columns present
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with StringIO() as fake_csv_with_ct_columns:
        fake_csv_with_ct_columns.write(
            ','.join(
                (
                    FIELD_ROOT_SAMPLE_ID,
                    FIELD_VIRAL_PREP_ID,
                    FIELD_RNA_ID,
                    FIELD_RNA_PCR_ID,
                    FIELD_RESULT,
                    FIELD_DATE_TESTED,
                    FIELD_LAB_ID,
                    FIELD_CH1_TARGET,
                    FIELD_CH1_RESULT,
                    FIELD_CH1_CQ,
                    FIELD_CH2_TARGET,
                    FIELD_CH2_RESULT,
                    FIELD_CH2_CQ,
                    FIELD_CH3_TARGET,
                    FIELD_CH3_RESULT,
                    FIELD_CH3_CQ,
                    FIELD_CH4_TARGET,
                    FIELD_CH4_RESULT,
                    FIELD_CH4_CQ
                )
            ) + '\n'
        )
        fake_csv_with_ct_columns.write(
            ','.join(
                (
                    "LTS00012216",
                    "AP-kfr-00057292_D11",
                    "AP-rna-1111_D11",
                    "CF91DLLK_D11",
                    "Positive",
                    "2020-07-20 07:54:34 UTC",
                    "AP",
                    "ORF1ab",
                    "Positive",
                    "12.46979445",
                    "N gene",
                    "Positive",
                    "13.2452244",
                    "S gene",
                    "Negative",
                    "",
                    "MS2",
                    "Positive",
                    "24.98589115\n"
                )
            )
        )
        fake_csv_with_ct_columns.seek(0)

        csv_to_test_reader = DictReader(fake_csv_with_ct_columns)

        expected_row = {
            "Root Sample ID": "LTS00012216",
            "Viral Prep ID": "AP-kfr-00057292_D11",
            "RNA ID": "AP-rna-1111_D11",
            "RNA-PCR ID": "CF91DLLK_D11",
            "Result": "Positive",
            "Date Tested": "2020-07-20 07:54:34 UTC",
            "Lab ID": "AP",
            "CH1-Target": "ORF1ab",
            "CH1-Result": "Positive",
            "CH1-Cq": "12.46979445",
            "CH2-Target": "N gene",
            "CH2-Result": "Positive",
            "CH2-Cq": "13.2452244",
            "CH3-Target": "S gene",
            "CH3-Result": "Negative",
            "CH4-Target": "MS2",
            "CH4-Result": "Positive",
            "CH4-Cq": "24.98589115",
        }

        assert centre_file.filtered_row(next(csv_to_test_reader), 2) == expected_row
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0

def test_parse_and_format_file_rows_to_add_file_details(config):
    timestamp = "some timestamp"
    centre_file = centre_file_with_mocked_filtered_postitive_identifier(config, "ASDF_200507_1340.csv")
    with patch.object(centre_file, "get_now_timestamp", return_value=timestamp):

        extra_fields_added = [
            {
                "Root Sample ID": "1",
                "RNA ID": "RNA_0043_H09",
                "plate_barcode": "RNA_0043",
                "source": "Alderley",
                "coordinate": "H09",
                "line_number": 2,
                "file_name": "ASDF_200507_1340.csv",
                "file_name_date": datetime(2020, 5, 7, 13, 40),
                "created_at": timestamp,
                "updated_at": timestamp,
                "Result": "Positive",
                "Lab ID": None,
                "filtered_positive": True,
                "filtered_positive_version": 'v2.3',
                "filtered_positive_timestamp": timestamp,
            },
            {
                "Root Sample ID": "2",
                "RNA ID": "RNA_0043_B08",
                "plate_barcode": "RNA_0043",
                "source": "Alderley",
                "coordinate": "B08",
                "line_number": 3,
                "file_name": "ASDF_200507_1340.csv",
                "file_name_date": datetime(2020, 5, 7, 13, 40),
                "created_at": timestamp,
                "updated_at": timestamp,
                "Result": "Negative",
                "Lab ID": None,
            },
        ]

        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID\n")
            fake_csv.write("1,RNA_0043_H09,Positive\n")
            fake_csv.write("2,RNA_0043_B08,Negative\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)

            assert augmented_data == extra_fields_added
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0

def test_parse_and_format_file_rows_detects_duplicates(config):
    timestamp = "some timestamp"
    centre_file = centre_file_with_mocked_filtered_postitive_identifier(config, "ASDF_200507_1340.csv")
    with patch.object(centre_file, "get_now_timestamp", return_value=timestamp):

        extra_fields_added = [
            {
                "Root Sample ID": "1",
                "RNA ID": "RNA_0043_H09",
                "plate_barcode": "RNA_0043",
                "source": "Alderley",
                "coordinate": "H09",
                "line_number": 2,
                "file_name": "ASDF_200507_1340.csv",
                "file_name_date": datetime(2020, 5, 7, 13, 40),
                "created_at": timestamp,
                "updated_at": timestamp,
                "Result": "Positive",
                "Lab ID": "Val",
                "filtered_positive": True,
                "filtered_positive_version": 'v2.3',
                "filtered_positive_timestamp": timestamp,
            },
        ]

        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID\n")
            fake_csv.write("1,RNA_0043_H09,Positive,Val\n")
            fake_csv.write("1,RNA_0043_H09,Positive,Val\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)
            assert augmented_data == extra_fields_added

            assert centre_file.logging_collection.aggregator_types["TYPE 5"].count_errors == 1
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0

def test_where_result_has_unexpected_value(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with patch.object(centre_file, "get_now_timestamp", return_value="some timestamp"):
        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID\n")

            # where row has valid value - should pass
            fake_csv.write("1,RNA_0043_H06,Positive,Val\n")

            # where row has valid value - should pass
            fake_csv.write("2,RNA_0043_H07,Negative,Val\n")

            # where row has valid value - should pass
            fake_csv.write("3,RNA_0043_H08,limit of detection,Val\n")

            # where row has valid value - should pass
            fake_csv.write("4,RNA_0043_H09,Void,Val\n")

            # where row has invalid value - should error
            fake_csv.write("5,RNA_0043_H10,NotAValidResult,Val\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)

            # should create a specific error type for the row
            assert centre_file.logging_collection.aggregator_types["TYPE 16"].count_errors == 1
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1

def test_where_ct_channel_target_has_unexpected_value(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with patch.object(centre_file, "get_now_timestamp", return_value="some timestamp"):
        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,CH1-Target\n")

            # where row has valid value - should pass
            fake_csv.write("1,RNA_0043_H09,Positive,Val,S gene\n")

            # where row is empty - should pass
            fake_csv.write("2,RNA_0043_H10,Positive,Val,\n")

            # where row has invalid value - should error
            fake_csv.write("2,RNA_0043_H11,Positive,Val,NotATarget\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)

            # should create a specific error type for the row
            assert centre_file.logging_collection.aggregator_types["TYPE 17"].count_errors == 1
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1


def test_where_ct_channel_result_has_unexpected_value(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with patch.object(centre_file, "get_now_timestamp", return_value="some timestamp"):
        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,CH1-Result\n")
            # row with valid value - should pass
            fake_csv.write("1,RNA_0043_H09,Negative,Val,Inconclusive\n")

            # row with invalid value - should error
            fake_csv.write("2,RNA_0043_H10,Negative,Val,NotAResult\n")

            # row with empty value - should pass
            fake_csv.write("2,RNA_0043_H11,Negative,Val,\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)

            # should create a specific error type for the row
            assert centre_file.logging_collection.aggregator_types["TYPE 18"].count_errors == 1
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1

def test_changes_ct_channel_cq_value_data_type(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with patch.object(centre_file, "get_now_timestamp", return_value="some timestamp"):
        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,CH1-Cq,CH2-Cq,CH3-Cq,CH4-Cq\n")

            fake_csv.write("1,RNA_0043_H09,Positive,Val,24.012833772,25.012833772,26.012833772,27.012833772\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)
            assert type(augmented_data[0][FIELD_CH1_CQ]) == Decimal128
            assert type(augmented_data[0][FIELD_CH2_CQ]) == Decimal128
            assert type(augmented_data[0][FIELD_CH3_CQ]) == Decimal128
            assert type(augmented_data[0][FIELD_CH4_CQ]) == Decimal128

def test_where_ct_channel_cq_value_is_not_numeric(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with patch.object(centre_file, "get_now_timestamp", return_value="some timestamp"):
        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,CH1-Cq\n")

            # where row has valid value - should pass
            fake_csv.write("1,RNA_0043_H09,Positive,Val,24.012833772\n")

            # where row has missing value - should pass
            fake_csv.write("1,RNA_0043_H09,Positive,Val,\n")

            # where row has invalid value - should error
            fake_csv.write("2,RNA_0043_H10,Positive,Val,NotANumber\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)

            # should create a specific error type for the row
            assert centre_file.logging_collection.aggregator_types["TYPE 19"].count_errors == 1
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1

def test_is_within_cq_range(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    assert centre_file.is_within_cq_range(Decimal('0.0'), Decimal('100.0'), Decimal128('0.0')) is True
    assert centre_file.is_within_cq_range(Decimal('0.0'), Decimal('100.0'), Decimal128('100.0')) is True
    assert centre_file.is_within_cq_range(Decimal('0.0'), Decimal('100.0'), Decimal128('27.019291283')) is True

    assert centre_file.is_within_cq_range(Decimal('0.0'), Decimal('100.0'), Decimal128('-0.00000001')) is False
    assert centre_file.is_within_cq_range(Decimal('0.0'), Decimal('100.0'), Decimal128('100.00000001')) is False

def test_where_ct_channel_cq_value_is_not_within_range(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with patch.object(centre_file, "get_now_timestamp", return_value="some timestamp"):
        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,CH1-Cq\n")

            # where row has valid value - should pass
            fake_csv.write("1,RNA_0043_H09,Positive,Val,24.012833772\n")

            # where row has missing value - should pass
            fake_csv.write("1,RNA_0043_H09,Positive,Val,\n")

            # where row has low value - should error
            fake_csv.write("2,RNA_0043_H10,Positive,Val,-12.18282273\n")

            # where row has low value - should error
            fake_csv.write("3,RNA_0043_H11,Positive,Val,100.01290002\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)

            # should create a specific error type for the row
            assert centre_file.logging_collection.aggregator_types["TYPE 20"].count_errors == 2
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 2

def test_where_positive_result_does_not_align_with_ct_channel_results(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with patch.object(centre_file, "get_now_timestamp", return_value="some timestamp"):
        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,CH1-Result,CH2-Result,CH3-Result,CH4-Result\n")

            # row where no values for channel results - should pass
            fake_csv.write("1,RNA_0043_H09,Positive,Val,,,,\n")

            # row where result value and channel values all positive - should pass
            fake_csv.write("2,RNA_0043_H10,Positive,Val,Positive,Positive,Positive,Positive\n")

            # row where channel values are mixed but at least one is positive - should pass
            fake_csv.write("3,RNA_0043_H11,Positive,Val,Positive,Negative,Inconclusive,Void\n")

            # row where channel values are all negative, inconclusive or void - should fail
            fake_csv.write("4,RNA_0043_H12,Positive,Val,Negative,Negative,Inconclusive,Void\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)

            # should create a specific error type for the row
            assert centre_file.logging_collection.aggregator_types["TYPE 21"].count_errors == 1
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1

def test_check_for_required_headers(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    # empty file
    with StringIO() as fake_csv:
        csv_to_test_reader = DictReader(fake_csv)
        assert centre_file.check_for_required_headers(csv_to_test_reader) is False
        assert centre_file.logging_collection.aggregator_types["TYPE 2"].count_errors == 1
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1

    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    # file with incorrect headers
    with StringIO() as fake_csv:
        fake_csv.write("id,RNA ID\n")
        fake_csv.write("1,RNA_0043_\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)

        assert centre_file.check_for_required_headers(csv_to_test_reader) is False
        assert centre_file.logging_collection.aggregator_types["TYPE 2"].count_errors == 1
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1

    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    # file with valid headers
    with StringIO() as fake_csv:
        fake_csv.write(
            f"{FIELD_ROOT_SAMPLE_ID},{FIELD_VIRAL_PREP_ID},{FIELD_RNA_ID},{FIELD_RNA_PCR_ID},"
            f"{FIELD_RESULT},{FIELD_DATE_TESTED},{FIELD_LAB_ID}\n"
        )
        fake_csv.write("1,0100000859NBC_B07,RNA_0043,CF06BAO5_B07,Positive,today,MK\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)

        assert centre_file.check_for_required_headers(csv_to_test_reader) is True
        assert centre_file.logging_collection.aggregator_types["TYPE 2"].count_errors == 0
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0

    # file with missing Lab ID header and add lab id false (default)
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with StringIO() as fake_csv_without_lab_id:
        fake_csv_without_lab_id.write(
            f"{FIELD_ROOT_SAMPLE_ID},{FIELD_VIRAL_PREP_ID},{FIELD_RNA_ID},{FIELD_RNA_PCR_ID},"
            f"{FIELD_RESULT},{FIELD_DATE_TESTED}\n"
        )
        fake_csv_without_lab_id.write("1,0100000859NBC_B07,RNA_0043,CF06BAO5_B07,Positive,today\n")
        fake_csv_without_lab_id.seek(0)

        csv_to_test_reader = DictReader(fake_csv_without_lab_id)

        assert centre_file.check_for_required_headers(csv_to_test_reader) is False
        assert centre_file.logging_collection.aggregator_types["TYPE 2"].count_errors == 1
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1

    # file with missing Lab ID header and add lab id true
    try:
        config.ADD_LAB_ID = True
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some_file.csv", centre)

        with StringIO() as fake_csv_without_lab_id:
            fake_csv_without_lab_id.write(
                f"{FIELD_ROOT_SAMPLE_ID},{FIELD_VIRAL_PREP_ID},{FIELD_RNA_ID},{FIELD_RNA_PCR_ID},"
                f"{FIELD_RESULT},{FIELD_DATE_TESTED}\n"
            )
            fake_csv_without_lab_id.write("1,0100000859NBC_B07,RNA_0043,CF06BAO5_B07,Positive,today\n")
            fake_csv_without_lab_id.seek(0)

            csv_to_test_reader = DictReader(fake_csv_without_lab_id)

            assert centre_file.check_for_required_headers(csv_to_test_reader) is True
            assert centre_file.logging_collection.aggregator_types["TYPE 2"].count_errors == 0
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0
    finally:
        config.ADD_LAB_ID = False

def test_backup_good_file(config, tmpdir):
    with patch.dict(config.CENTRES[0], {"backups_folder": tmpdir.realpath()}):
        # create temporary success and errors folders for the files to end up in
        success_folder = tmpdir.mkdir(SUCCESSES_DIR)
        errors_folder = tmpdir.mkdir(ERRORS_DIR)

        # checks that they are empty
        assert len(success_folder.listdir()) == 0
        assert len(errors_folder.listdir()) == 0

        # configure to use the backups folder for this test
        centre = Centre(config, config.CENTRES[0])

        # create a file inside the centre download dir
        filename = "AP_sanger_report_200503_2338.csv"

        # test the backup of the file to the successes folder
        centre_file = CentreFile(filename, centre)
        centre_file.backup_file()

        assert len(success_folder.listdir()) == 1
        assert len(errors_folder.listdir()) == 0

        filename_with_timestamp = os.path.basename(success_folder.listdir()[0])
        assert filename in filename_with_timestamp

def test_backup_bad_file(config, tmpdir):
    with patch.dict(config.CENTRES[0], {"backups_folder": tmpdir.realpath()}):
        # create temporary success and errors folders for the files to end up in
        success_folder = tmpdir.mkdir(SUCCESSES_DIR)
        errors_folder = tmpdir.mkdir(ERRORS_DIR)

        # checks that they are empty
        assert len(success_folder.listdir()) == 0
        assert len(errors_folder.listdir()) == 0

        # configure to use the backups folder for this test
        centre = Centre(config, config.CENTRES[0])

        # create a file inside the centre download dir
        filename = "AP_sanger_report_200518_2132.csv"

        # test the backup of the file to the errors folder
        centre_file = CentreFile(filename, centre)
        centre_file.logging_collection.add_error("TYPE 4", "Some error happened")
        centre_file.backup_file()

        assert len(errors_folder.listdir()) == 1
        assert len(success_folder.listdir()) == 0

        filename_with_timestamp = os.path.basename(errors_folder.listdir()[0])
        assert filename in filename_with_timestamp

# tests for parsing file name date
def test_file_name_date_parses_right(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)
    assert centre_file.file_name_date().year == 2020
    assert centre_file.file_name_date().month == 5
    assert centre_file.file_name_date().day == 3
    assert centre_file.file_name_date().hour == 23
    assert centre_file.file_name_date().minute == 38

    centre_file = CentreFile("AP_sanger_report_200503_2338 (2).csv", centre)
    assert centre_file.file_name_date() == None

def test_set_state_for_file_when_file_in_black_list(config, blacklist_for_centre, testing_centres):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)
    centre_file.set_state_for_file()

    assert centre_file.file_state == CentreFileState.FILE_IN_BLACKLIST

def test_set_state_for_file_when_never_seen_before(config, testing_centres):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)
    centre_file.set_state_for_file()

    assert centre_file.file_state == CentreFileState.FILE_NOT_PROCESSED_YET

def test_set_state_for_file_when_in_error_folder(config, tmpdir, testing_centres):
    with patch.dict(config.CENTRES[0], {"backups_folder": tmpdir.realpath()}):
        errors_folder = tmpdir.mkdir(ERRORS_DIR)
        success_folder = tmpdir.mkdir(SUCCESSES_DIR)

        # configure to use the backups folder for this test

        centre = Centre(config, config.CENTRES[0])

        # create a backup of the file inside the errors directory as if previously processed there
        filename = "AP_sanger_report_200518_2132.csv"
        centre_file = CentreFile(filename, centre)
        centre_file.logging_collection.add_error("TYPE 4", "Some error happened")
        centre_file.backup_file()

        assert len(errors_folder.listdir()) == 1

        # check the file state again now the error version exists
        centre_file.set_state_for_file()

        assert centre_file.file_state == CentreFileState.FILE_PROCESSED_WITH_ERROR

def test_set_state_for_file_when_in_success_folder(config):
    return False

# tests for inserting docs into mlwh using rows with and without ct columns
def test_insert_samples_from_docs_into_mlwh(config, mlwh_connection):
    with patch('crawler.db.create_mysql_connection', return_value = 'not none'):
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)

        docs = [
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000004',
                FIELD_RNA_ID: 'TC-rna-00000029_H11',
                FIELD_PLATE_BARCODE: 'TC-rna-00000029',
                FIELD_COORDINATE: 'H11',
                FIELD_RESULT: 'Negative',
                FIELD_DATE_TESTED: '2020-04-23 14:40:00 UTC',
                FIELD_SOURCE: 'Test Centre',
                FIELD_LAB_ID: 'TC'
            },
            {
                '_id': ObjectId('5f562d9931d9959b92544729'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000005',
                FIELD_RNA_ID: 'TC-rna-00000029_H12',
                FIELD_PLATE_BARCODE: 'TC-rna-00000029',
                FIELD_COORDINATE: 'H12',
                FIELD_RESULT: POSITIVE_RESULT_VALUE,
                FIELD_DATE_TESTED: '2020-04-23 14:41:00 UTC',
                FIELD_SOURCE: 'Test Centre',
                FIELD_LAB_ID: 'TC',
                FIELD_CH1_TARGET: 'ORF1ab',
                FIELD_CH1_RESULT: POSITIVE_RESULT_VALUE,
                FIELD_CH1_CQ: Decimal128('21.28726211'),
                FIELD_CH2_TARGET: 'N gene',
                FIELD_CH2_RESULT: POSITIVE_RESULT_VALUE,
                FIELD_CH2_CQ: Decimal128('18.12736661'),
                FIELD_CH3_TARGET: 'S gene',
                FIELD_CH3_RESULT: POSITIVE_RESULT_VALUE,
                FIELD_CH3_CQ: Decimal128('22.63616273'),
                FIELD_CH4_TARGET: 'MS2',
                FIELD_CH4_RESULT: POSITIVE_RESULT_VALUE,
                FIELD_CH4_CQ: Decimal128('26.25125612'),
                FIELD_FILTERED_POSITIVE: True,
                FIELD_FILTERED_POSITIVE_VERSION: 'v2.3',
                FIELD_FILTERED_POSITIVE_TIMESTAMP: '2020-04-23 14:41:00 UTC',
            }
        ]

        centre_file.insert_samples_from_docs_into_mlwh(docs)

        error_count = centre_file.logging_collection.get_count_of_all_errors_and_criticals()
        error_messages = centre_file.logging_collection.get_aggregate_messages()
        assert error_count == 0, f"Should not be any errors. Actual number errors: {error_count}. Error details: {error_messages}"

        cursor = mlwh_connection.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME}")
        rows = cursor.fetchall()
        cursor.close()

        assert rows[0][MLWH_MONGODB_ID] == '5f562d9931d9959b92544728'
        assert rows[0][MLWH_ROOT_SAMPLE_ID] == 'ABC00000004'
        assert rows[0][MLWH_RNA_ID] == 'TC-rna-00000029_H11'
        assert rows[0][MLWH_PLATE_BARCODE] == 'TC-rna-00000029'
        assert rows[0][MLWH_COORDINATE] == 'H11'
        assert rows[0][MLWH_RESULT] == 'Negative'
        assert rows[0][MLWH_DATE_TESTED_STRING] == '2020-04-23 14:40:00 UTC'
        assert rows[0][MLWH_DATE_TESTED] == datetime(2020, 4, 23, 14, 40, 0)
        assert rows[0][MLWH_SOURCE] == 'Test Centre'
        assert rows[0][MLWH_LAB_ID] == 'TC'
        assert rows[0][MLWH_CH1_TARGET] is None
        assert rows[0][MLWH_CH1_RESULT] is None
        assert rows[0][MLWH_CH1_CQ] is None
        assert rows[0][MLWH_CH2_TARGET] is None
        assert rows[0][MLWH_CH2_RESULT] is None
        assert rows[0][MLWH_CH2_CQ] is None
        assert rows[0][MLWH_CH3_TARGET] is None
        assert rows[0][MLWH_CH3_RESULT] is None
        assert rows[0][MLWH_CH3_CQ] is None
        assert rows[0][MLWH_CH4_TARGET] is None
        assert rows[0][MLWH_CH4_RESULT] is None
        assert rows[0][MLWH_CH4_CQ] is None
        assert rows[0][MLWH_FILTERED_POSITIVE] is None
        assert rows[0][MLWH_FILTERED_POSITIVE_VERSION] is None
        assert rows[0][MLWH_FILTERED_POSITIVE_TIMESTAMP] is None
        assert rows[0][MLWH_CREATED_AT] is not None
        assert rows[0][MLWH_UPDATED_AT] is not None

        assert rows[1][MLWH_MONGODB_ID] == '5f562d9931d9959b92544729'
        assert rows[1][MLWH_ROOT_SAMPLE_ID] == 'ABC00000005'
        assert rows[1][MLWH_RNA_ID] == 'TC-rna-00000029_H12'
        assert rows[1][MLWH_PLATE_BARCODE] == 'TC-rna-00000029'
        assert rows[1][MLWH_COORDINATE] == 'H12'
        assert rows[1][MLWH_RESULT] == POSITIVE_RESULT_VALUE
        assert rows[1][MLWH_DATE_TESTED_STRING] == '2020-04-23 14:41:00 UTC'
        assert rows[1][MLWH_DATE_TESTED] == datetime(2020, 4, 23, 14, 41, 0)
        assert rows[1][MLWH_SOURCE] == 'Test Centre'
        assert rows[1][MLWH_LAB_ID] == 'TC'
        assert rows[1][MLWH_CH1_TARGET] == 'ORF1ab'
        assert rows[1][MLWH_CH1_RESULT] == POSITIVE_RESULT_VALUE
        assert rows[1][MLWH_CH1_CQ] == Decimal('21.28726211')
        assert rows[1][MLWH_CH2_TARGET] == 'N gene'
        assert rows[1][MLWH_CH2_RESULT] == POSITIVE_RESULT_VALUE
        assert rows[1][MLWH_CH2_CQ] == Decimal('18.12736661')
        assert rows[1][MLWH_CH3_TARGET] == 'S gene'
        assert rows[1][MLWH_CH3_RESULT] == POSITIVE_RESULT_VALUE
        assert rows[1][MLWH_CH3_CQ] == Decimal('22.63616273')
        assert rows[1][MLWH_CH4_TARGET] == 'MS2'
        assert rows[1][MLWH_CH4_RESULT] == POSITIVE_RESULT_VALUE
        assert rows[1][MLWH_CH4_CQ] == Decimal('26.25125612')
        assert rows[1][MLWH_FILTERED_POSITIVE] == 1
        assert rows[1][MLWH_FILTERED_POSITIVE_VERSION] == 'v2.3'
        assert rows[1][MLWH_FILTERED_POSITIVE_TIMESTAMP] == datetime(2020, 4, 23, 14, 41, 0)
        assert rows[1][MLWH_CREATED_AT] is not None
        assert rows[1][MLWH_UPDATED_AT] is not None

def test_insert_samples_from_docs_into_mlwh_date_tested_missing(config, mlwh_connection):
    with patch('crawler.db.create_mysql_connection', return_value = 'not none'):
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)

        docs = [
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000004',
                FIELD_RNA_ID: 'TC-rna-00000029_H11',
                FIELD_PLATE_BARCODE: 'TC-rna-00000029',
                FIELD_COORDINATE: 'H11',
                FIELD_RESULT: 'Negative',
                FIELD_SOURCE: 'Test Centre',
                FIELD_LAB_ID: 'TC'
            }
        ]

        centre_file.insert_samples_from_docs_into_mlwh(docs)

        error_count = centre_file.logging_collection.get_count_of_all_errors_and_criticals()
        error_messages = centre_file.logging_collection.get_aggregate_messages()
        assert error_count == 0, f"Should not be any errors. Actual number errors: {error_count}. Error details: {error_messages}"

        cursor = mlwh_connection.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME}")
        rows = cursor.fetchall()
        cursor.close()

        assert rows[0][MLWH_DATE_TESTED] == None

def test_insert_samples_from_docs_into_mlwh_date_tested_blank(config, mlwh_connection):
    with patch('crawler.db.create_mysql_connection', return_value = 'not none'):
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)

        docs = [
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000004',
                FIELD_RNA_ID: 'TC-rna-00000029_H11',
                FIELD_PLATE_BARCODE: 'TC-rna-00000029',
                FIELD_COORDINATE: 'H11',
                FIELD_RESULT: 'Negative',
                FIELD_DATE_TESTED: '',
                FIELD_SOURCE: 'Test Centre',
                FIELD_LAB_ID: 'TC'
            }
        ]

        centre_file.insert_samples_from_docs_into_mlwh(docs)

        error_count = centre_file.logging_collection.get_count_of_all_errors_and_criticals()
        error_messages = centre_file.logging_collection.get_aggregate_messages()
        assert error_count == 0, f"Should not be any errors. Actual number errors: {error_count}. Error details: {error_messages}"

        cursor = mlwh_connection.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME}")
        rows = cursor.fetchall()
        cursor.close()

        assert rows[0][MLWH_DATE_TESTED] == None

def test_calculate_dart_well_index(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    sample = None
    assert centre_file.calculate_dart_well_index(sample) == None, "Expected to be unable to determine a well index for no sample"

    sample = {}
    assert centre_file.calculate_dart_well_index(sample) == None, "Expected to be unable to determine a well index for sample without a coordinate"

    sample = { FIELD_COORDINATE: '01A' }
    assert centre_file.calculate_dart_well_index(sample) == None, "Expected to be unable to determine a well index for sample with invalid coordinate"

    sample = { FIELD_COORDINATE: 'A00' }
    assert centre_file.calculate_dart_well_index(sample) == None, "Expected to be unable to determine a well index for sample with coordinate column out of range"

    sample = { FIELD_COORDINATE: 'Q01' }
    assert centre_file.calculate_dart_well_index(sample) == None, "Expected to be unable to determine a well index for sample with coordinate row out of range"

    sample = { FIELD_COORDINATE: 'B7' }
    assert centre_file.calculate_dart_well_index(sample) == 19, "Expected well index of 19"

    sample = { FIELD_COORDINATE: 'F03' }
    assert centre_file.calculate_dart_well_index(sample) == 63, "Expected well index of 63"

# tests for inserting docs into DART
def test_insert_plates_and_wells_from_docs_into_dart_none_connection(config):
    with patch('crawler.file_processing.create_dart_sql_server_conn', return_value = None):
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)
        centre_file.insert_plates_and_wells_from_docs_into_dart([])

        # logs error on failing to initialise the SQL server connection
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
        assert centre_file.logging_collection.aggregator_types["TYPE 24"].count_errors == 1

def test_insert_plates_and_wells_from_docs_into_dart_failed_cursor(config):
    with patch('crawler.file_processing.create_dart_sql_server_conn') as mock_conn:
        mock_conn().cursor = MagicMock(side_effect = Exception('Boom!'))
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)
        centre_file.insert_plates_and_wells_from_docs_into_dart([])

        # logs error on failing to initialise the SQL server cursor
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
        assert centre_file.logging_collection.aggregator_types["TYPE 23"].count_errors == 1
        mock_conn().close.assert_called_once()

def test_insert_plates_and_wells_from_docs_into_dart_failed_cursor_execute(config):
    with patch('crawler.file_processing.create_dart_sql_server_conn') as mock_conn:
        mock_conn().cursor().execute = MagicMock(side_effect = Exception('Boom!'))
        docs_to_insert = [
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000004',
                FIELD_RNA_ID: 'TC-rna-00000029_H11',
                FIELD_PLATE_BARCODE: 'TC-rna-00000029',
                FIELD_COORDINATE: 'H11',
                FIELD_RESULT: POSITIVE_RESULT_VALUE,
            }
        ]

        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)
        centre_file.insert_plates_and_wells_from_docs_into_dart(docs_to_insert)

        # logs error and rolls back on exception calling the plate stored procedure
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
        assert centre_file.logging_collection.aggregator_types["TYPE 22"].count_errors == 1
        mock_conn().cursor().rollback.assert_called_once()
        mock_conn().cursor().commit.assert_not_called()
        mock_conn().close.assert_called_once()

def test_insert_plates_and_wells_from_docs_into_dart_none_well_index(config):
    with patch('crawler.file_processing.create_dart_sql_server_conn') as mock_conn:
        docs_to_insert = [
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000004',
                FIELD_RNA_ID: 'TC-rna-00000029_H11',
                FIELD_PLATE_BARCODE: 'TC-rna-00000029',
                FIELD_COORDINATE: 'H11',
                FIELD_RESULT: POSITIVE_RESULT_VALUE,
            }
        ]

        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)
        centre_file.calculate_dart_well_index = MagicMock(return_value = None)
        centre_file.insert_plates_and_wells_from_docs_into_dart(docs_to_insert)

        # adds plate, but logs error on unable to determine well index
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
        assert centre_file.logging_collection.aggregator_types["TYPE 25"].count_errors == 1
        mock_conn().cursor().rollback.assert_not_called()
        mock_conn().cursor().commit.assert_called_once()
        mock_conn().close.assert_called_once()

def test_insert_plates_and_wells_from_docs_into_dart_multiple_plates(config):
    with patch('crawler.file_processing.create_dart_sql_server_conn') as mock_conn:
        docs_to_insert = [
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000004',
                FIELD_RNA_ID: 'TC-rna-00000029_A01',
                FIELD_PLATE_BARCODE: 'TC-rna-00000029',
                FIELD_COORDINATE: 'A01',
                FIELD_LAB_ID: 'AP',
                FIELD_RESULT: POSITIVE_RESULT_VALUE,
                'well_index': 1
            },
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000006',
                FIELD_RNA_ID: 'TC-rna-00000024_B01',
                FIELD_PLATE_BARCODE: 'TC-rna-00000024',
                FIELD_COORDINATE: 'B01',
                FIELD_LAB_ID: 'AP',
                FIELD_RESULT: POSITIVE_RESULT_VALUE,
                'well_index': 13
            }
        ]

        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)
        centre_file.insert_plates_and_wells_from_docs_into_dart(docs_to_insert)

        # adds plates and wells as expected
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0
        assert mock_conn().cursor().execute.call_count == 10
        for doc in docs_to_insert:
            plate_barcode = doc[FIELD_PLATE_BARCODE]
            well_index = doc['well_index']
            mock_conn().cursor().execute.assert_any_call('{CALL dbo.plDART_PlateCreate (?,?,?)}', (plate_barcode, centre_file.centre_config["biomek_labware_class"], 96))
            mock_conn().cursor().execute.assert_any_call("{CALL dbo.plDART_PlateUpdateWell (?,?,?,?)}", (plate_barcode, 'state', '', well_index))
            mock_conn().cursor().execute.assert_any_call("{CALL dbo.plDART_PlateUpdateWell (?,?,?,?)}", (plate_barcode, 'root_sample_id', doc[FIELD_ROOT_SAMPLE_ID], well_index))
            mock_conn().cursor().execute.assert_any_call("{CALL dbo.plDART_PlateUpdateWell (?,?,?,?)}", (plate_barcode, 'rna_id', doc[FIELD_RNA_ID], well_index))
            mock_conn().cursor().execute.assert_any_call("{CALL dbo.plDART_PlateUpdateWell (?,?,?,?)}", (plate_barcode, 'lab_id', doc[FIELD_LAB_ID], well_index))
        mock_conn().cursor().rollback.assert_not_called()
        assert mock_conn().cursor().commit.call_count == 2
        mock_conn().close.assert_called_once()

def test_insert_plates_and_wells_from_docs_into_dart_single_plate_multiple_wells(config):
    with patch('crawler.file_processing.create_dart_sql_server_conn') as mock_conn:
        plate_barcode = 'TC-rna-00000029'
        docs_to_insert = [
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000004',
                FIELD_RNA_ID: f'{plate_barcode}_A01',
                FIELD_PLATE_BARCODE: plate_barcode,
                FIELD_COORDINATE: 'A01',
                FIELD_LAB_ID: 'AP',
                FIELD_RESULT: POSITIVE_RESULT_VALUE,
                'well_index': 1
            },
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000006',
                FIELD_RNA_ID: f'{plate_barcode}_A02',
                FIELD_PLATE_BARCODE: plate_barcode,
                FIELD_COORDINATE: 'A02',
                FIELD_LAB_ID: 'AP',
                FIELD_RESULT: POSITIVE_RESULT_VALUE,
                'well_index': 2
            }
        ]

        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)
        centre_file.insert_plates_and_wells_from_docs_into_dart(docs_to_insert)

        # adds plate and wells as expected
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0
        assert mock_conn().cursor().execute.call_count == 9
        mock_conn().cursor().execute.assert_any_call('{CALL dbo.plDART_PlateCreate (?,?,?)}', (plate_barcode, centre_file.centre_config["biomek_labware_class"], 96))
        for doc in docs_to_insert:
            well_index = doc['well_index']
            mock_conn().cursor().execute.assert_any_call("{CALL dbo.plDART_PlateUpdateWell (?,?,?,?)}", (plate_barcode, 'state', '', well_index))
            mock_conn().cursor().execute.assert_any_call("{CALL dbo.plDART_PlateUpdateWell (?,?,?,?)}", (plate_barcode, 'root_sample_id', doc[FIELD_ROOT_SAMPLE_ID], well_index))
            mock_conn().cursor().execute.assert_any_call("{CALL dbo.plDART_PlateUpdateWell (?,?,?,?)}", (plate_barcode, 'rna_id', doc[FIELD_RNA_ID], well_index))
            mock_conn().cursor().execute.assert_any_call("{CALL dbo.plDART_PlateUpdateWell (?,?,?,?)}", (plate_barcode, 'lab_id', doc[FIELD_LAB_ID], well_index))
        mock_conn().cursor().rollback.assert_not_called()
        assert mock_conn().cursor().commit.call_count == 1
        mock_conn().close.assert_called_once()

def test_insert_plates_and_wells_from_docs_into_dart_sets_well_state(config):
    with patch('crawler.file_processing.create_dart_sql_server_conn') as mock_conn:
        plate_barcode = 'TC-rna-00000029'
        docs_to_insert = [
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000004',
                FIELD_RNA_ID: f'{plate_barcode}_A01',
                FIELD_PLATE_BARCODE: plate_barcode,
                FIELD_COORDINATE: 'A01',
                FIELD_LAB_ID: 'AP',
                FIELD_RESULT: POSITIVE_RESULT_VALUE,
                'well_index': 1,
                'state': ''
            },
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000006',
                FIELD_RNA_ID: f'{plate_barcode}_A02',
                FIELD_PLATE_BARCODE: plate_barcode,
                FIELD_COORDINATE: 'A02',
                FIELD_LAB_ID: 'AP',
                FIELD_RESULT: POSITIVE_RESULT_VALUE,
                FIELD_FILTERED_POSITIVE: False,
                'well_index': 2,
                'state': ''
            },
            {
                '_id': ObjectId('5f562d9931d9959b92544728'),
                FIELD_ROOT_SAMPLE_ID: 'ABC00000008',
                FIELD_RNA_ID: f'{plate_barcode}_A03',
                FIELD_PLATE_BARCODE: plate_barcode,
                FIELD_COORDINATE: 'A03',
                FIELD_LAB_ID: 'AP',
                FIELD_RESULT: POSITIVE_RESULT_VALUE,
                FIELD_FILTERED_POSITIVE: True,
                'well_index': 3,
                'state': 'pickable'
            }
        ]

        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)
        centre_file.insert_plates_and_wells_from_docs_into_dart(docs_to_insert)

        # adds plate and wells as expected
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0
        for doc in docs_to_insert:
            mock_conn().cursor().execute.assert_any_call("{CALL dbo.plDART_PlateUpdateWell (?,?,?,?)}", (plate_barcode, 'state', doc['state'], doc['well_index']))
        mock_conn().cursor().rollback.assert_not_called()
        assert mock_conn().cursor().commit.call_count == 1
        mock_conn().close.assert_called_once()