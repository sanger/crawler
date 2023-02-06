import os
import uuid
from csv import DictReader
from datetime import datetime
from decimal import Decimal
from io import StringIO
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from mysql.connector.connection_cext import CMySQLConnection

from crawler.constants import (
    CENTRE_KEY_BACKUPS_FOLDER,
    CENTRE_KEY_BARCODE_FIELD,
    CENTRE_KEY_BARCODE_REGEX,
    CENTRE_KEY_BIOMEK_LABWARE_CLASS,
    CENTRE_KEY_PREFIX,
    CENTRE_KEY_SFTP_ROOT_READ,
    COLLECTION_IMPORTS,
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    DART_STATE_PENDING,
    FIELD_BARCODE,
    FIELD_CH1_CQ,
    FIELD_CH1_RESULT,
    FIELD_CH1_TARGET,
    FIELD_CH2_CQ,
    FIELD_CH2_RESULT,
    FIELD_CH2_TARGET,
    FIELD_CH3_CQ,
    FIELD_CH3_RESULT,
    FIELD_CH3_TARGET,
    FIELD_CH4_CQ,
    FIELD_CH4_RESULT,
    FIELD_CH4_TARGET,
    FIELD_COORDINATE,
    FIELD_CREATED_AT,
    FIELD_DATE_TESTED,
    FIELD_FILE_NAME,
    FIELD_FILE_NAME_DATE,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_LAB_ID,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_LINE_NUMBER,
    FIELD_MONGO_COG_UK_ID,
    FIELD_MONGO_LAB_ID,
    FIELD_MUST_SEQUENCE,
    FIELD_PLATE_BARCODE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_RNA_PCR_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SOURCE,
    FIELD_UPDATED_AT,
    FIELD_VIRAL_PREP_ID,
    MLWH_CH1_CQ,
    MLWH_CH1_RESULT,
    MLWH_CH1_TARGET,
    MLWH_CH2_CQ,
    MLWH_CH2_RESULT,
    MLWH_CH2_TARGET,
    MLWH_CH3_CQ,
    MLWH_CH3_RESULT,
    MLWH_CH3_TARGET,
    MLWH_CH4_CQ,
    MLWH_CH4_RESULT,
    MLWH_CH4_TARGET,
    MLWH_COG_UK_ID,
    MLWH_COORDINATE,
    MLWH_CREATED_AT,
    MLWH_DATE_TESTED,
    MLWH_FILTERED_POSITIVE,
    MLWH_FILTERED_POSITIVE_TIMESTAMP,
    MLWH_FILTERED_POSITIVE_VERSION,
    MLWH_IS_CURRENT,
    MLWH_LAB_ID,
    MLWH_MONGODB_ID,
    MLWH_MUST_SEQUENCE,
    MLWH_PLATE_BARCODE,
    MLWH_PREFERENTIALLY_SEQUENCE,
    MLWH_RESULT,
    MLWH_RNA_ID,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_SOURCE,
    MLWH_TABLE_NAME,
    MLWH_UPDATED_AT,
    RESULT_VALUE_POSITIVE,
)
from crawler.db.mongo import get_mongo_collection
from crawler.file_processing import ERRORS_DIR, SUCCESSES_DIR, Centre, CentreFile
from crawler.helpers.general_helpers import get_sftp_connection
from crawler.types import Config, ModifiedRow, SampleDoc
from tests.conftest import MockedError, generate_new_object_for_string

# ----- tests helpers -----


def centre_file_with_mocked_filtered_positive_identifier(config, file_name):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile(file_name, centre)
    centre_file.filtered_positive_identifier.version = "v2.3"
    centre_file.filtered_positive_identifier.is_positive = MagicMock(return_value=True)  # type: ignore
    return centre_file


# ----- tests for class Centre -----


def test_get_download_dir(config):
    for centre_config in config.CENTRES:
        centre = Centre(config, centre_config)

        assert centre.get_download_dir() == f"{config.DIR_DOWNLOADED_DATA}{centre_config[CENTRE_KEY_PREFIX]}/"


def test_process_files(mongo_database, config, baracoda, testing_files_for_process, testing_centres, pyodbc_conn):
    _, mongo_database = mongo_database

    centre_config = config.CENTRES[3]
    centre_config[CENTRE_KEY_SFTP_ROOT_READ] = "tmp/files"
    centre = Centre(config, centre_config)
    centre.process_files(True)

    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)

    pyodbc_conn.assert_called()

    # We record *all* our samples
    date_time = datetime(year=2020, month=4, day=16, hour=14, minute=30, second=40)
    assert (
        samples_collection.count_documents(
            {"RNA ID": "CB123_A09", "source": "Cambridge-az", FIELD_DATE_TESTED: date_time}
        )
        == 1
    )
    assert source_plates_collection.count_documents({"barcode": "CB123"}) == 1


def test_process_files_dont_add_to_dart_flag_not_set(
    mongo_database, config, testing_files_for_process, testing_centres, pyodbc_conn
):
    _, mongo_database = mongo_database

    centre_config = config.CENTRES[0]
    centre_config["sftp_root_read"] = "tmp/files"
    centre = Centre(config, centre_config)
    centre.process_files(False)

    # assert no attempt was made to connect
    pyodbc_conn.assert_not_called()


def test_process_files_dont_add_to_dart_mlwh_failed(
    mongo_database, config, testing_files_for_process, testing_centres, pyodbc_conn
):
    with patch("crawler.db.mysql.run_mysql_executemany_query", side_effect=MockedError("Boom!")):
        _, mongo_database = mongo_database

        centre_config = config.CENTRES[0]
        centre_config["sftp_root_read"] = "tmp/files"
        centre = Centre(config, centre_config)
        centre.process_files(True)

        # assert no attempt was made to connect
        pyodbc_conn.assert_not_called()


def test_process_files_correctly_handles_files_not_to_be_processed(
    mongo_database, config, testing_files_for_process, testing_centres
):
    _, mongo_database = mongo_database

    centre_config = config.CENTRES[0]
    centre_config["sftp_root_read"] = "tmp/files"
    centre = Centre(config, centre_config)
    centre.process_files(True)

    # No samples were recorded
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    assert samples_collection.count_documents({}) == 0

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    assert source_plates_collection.count_documents({}) == 0

    # Import records were made indicating files were not processed
    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    assert imports_collection.count_documents({}) == 3

    imports = imports_collection.find()
    for imp in imports:
        assert len(imp["errors"]) == 2
        assert all("TYPE 34" in err for err in imp["errors"])

    # Assert that files were stored in backups as errors
    errors_path = os.path.join(centre_config[CENTRE_KEY_BACKUPS_FOLDER], ERRORS_DIR)
    assert len(os.listdir(errors_path)) == 3


def test_process_files_one_wrong_format(mongo_database, config, baracoda, testing_files_for_process, testing_centres):
    """Test using files in the files/TEST directory; they include a rogue XLSX file dressed as CSV file."""
    _, mongo_database = mongo_database

    # get the TEST centre
    centre_config = next(filter(lambda centre: centre[CENTRE_KEY_PREFIX] == "TEST", config.CENTRES))
    centre_config["sftp_root_read"] = "tmp/files"
    centre = Centre(config, centre_config)
    centre.process_files(add_to_dart=False)

    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)

    # check that the valid file still gets processed, even though the bad file is in there
    assert samples_collection.count_documents({"RNA ID": "TS789_A02", "source": "Test Centre"}) == 1

    assert imports_collection.count_documents({"csv_file_used": "TEST_sanger_report_200518_2207.csv"}) == 1
    for i in imports_collection.find({"csv_file_used": "TEST_sanger_report_200518_2207.csv"}):
        assert "CRITICAL: File is unexpected type and cannot be processed. (TYPE 10)" in i["errors"]


UNCONSOLIDATED_SURVEILLANCE_FILENAME = "AP_sanger_report_200503_2338.csv"
CONSOLIDATED_EAGLE_FILENAME = "APE200503.csv"
CONSOLIDATED_SURVEILLANCE_FILENAME = "AP-rna-200503.csv"
MK_CONSOLIDATED_SURVEILLANCE_FILENAME1 = "RNA123456.csv"
MK_CONSOLIDATED_SURVEILLANCE_FILENAME2 = "cpRNA123456.csv"
AP_CONSOLIDATED_SURVEILLANCE_FILENAME1 = "AP-chp-12345678.csv"
AP_CONSOLIDATED_SURVEILLANCE_FILENAME2 = "AP-rna-123456.csv"
GLASGOW_CONSOLIDATED_SURVEILLANCE_FILENAME1 = "GLA123456a.csv"
GLASGOW_CONSOLIDATED_SURVEILLANCE_FILENAME2 = "GLS-GP-123456.csv"
HSLL_CONSOLIDATED_SURVEILLANCE_FILENAME1 = "HSL123456.csv"
HSLL_CONSOLIDATED_SURVEILLANCE_FILENAME2 = "HSL123456789.csv"


@pytest.mark.parametrize(
    "filename, centre_config_index, expected_value",
    [
        [UNCONSOLIDATED_SURVEILLANCE_FILENAME, 0, True],
        [CONSOLIDATED_EAGLE_FILENAME, 0, True],
        [AP_CONSOLIDATED_SURVEILLANCE_FILENAME1, 0, True],
        [AP_CONSOLIDATED_SURVEILLANCE_FILENAME2, 0, True],
        ["AP-123456.csv", 1, False],
        [MK_CONSOLIDATED_SURVEILLANCE_FILENAME1, 1, True],
        [MK_CONSOLIDATED_SURVEILLANCE_FILENAME2, 1, True],
        ["MK-200503.csv", 1, False],
        [GLASGOW_CONSOLIDATED_SURVEILLANCE_FILENAME1, 2, True],
        [GLASGOW_CONSOLIDATED_SURVEILLANCE_FILENAME2, 2, True],
        ["GLS-123456.csv", 2, False],
        [HSLL_CONSOLIDATED_SURVEILLANCE_FILENAME1, 5, True],
        [HSLL_CONSOLIDATED_SURVEILLANCE_FILENAME2, 5, True],
        ["HSL32334r.csv", 5, False],
    ],
)
def test_can_identify_valid_filename(config, filename, centre_config_index, expected_value):
    centre = Centre(config, config.CENTRES[centre_config_index])
    assert centre.is_valid_filename(filename) is expected_value


@pytest.mark.parametrize(
    "filename, expected_value",
    [
        [UNCONSOLIDATED_SURVEILLANCE_FILENAME, False],
        [CONSOLIDATED_EAGLE_FILENAME, True],
        [CONSOLIDATED_SURVEILLANCE_FILENAME, True],
    ],
)
def test_can_identify_consolidated_filename(config, filename, expected_value):
    centre = Centre(config, config.CENTRES[0])
    assert centre.is_consolidated_filename(filename) is expected_value


@pytest.mark.parametrize(
    "filename, expected_value",
    [
        [UNCONSOLIDATED_SURVEILLANCE_FILENAME, False],
        [CONSOLIDATED_EAGLE_FILENAME, True],
        [CONSOLIDATED_SURVEILLANCE_FILENAME, False],
    ],
)
def test_can_identify_eagle_filename(config, filename, expected_value):
    centre = Centre(config, config.CENTRES[0])
    assert centre.is_eagle_filename(filename) is expected_value


@pytest.mark.parametrize(
    "filename, expected_value",
    [
        [UNCONSOLIDATED_SURVEILLANCE_FILENAME, False],
        [CONSOLIDATED_EAGLE_FILENAME, False],
        [CONSOLIDATED_SURVEILLANCE_FILENAME, True],
    ],
)
def test_can_identify_consolidated_surveillance_filename(config, filename, expected_value):
    centre = Centre(config, config.CENTRES[0])
    assert centre.is_consolidated_surveillance_filename(filename) is expected_value


@pytest.mark.parametrize(
    "filename, expected_value",
    [
        [UNCONSOLIDATED_SURVEILLANCE_FILENAME, True],
        [CONSOLIDATED_EAGLE_FILENAME, False],
        [CONSOLIDATED_SURVEILLANCE_FILENAME, True],
    ],
)
def test_can_identify_surveillance_filename(config, filename, expected_value):
    centre = Centre(config, config.CENTRES[0])
    assert centre.is_surveillance_filename(filename) is expected_value


def test_process_files_with_whitespace(mongo_database, config, baracoda, testing_files_for_process, testing_centres):
    """Test using files in the files/TEST directory; they include a file with lots of whitespace."""
    _, mongo_database = mongo_database

    # get the TEST centre
    centre_config = next(filter(lambda centre: centre[CENTRE_KEY_PREFIX] == "TEST", config.CENTRES))
    centre_config["sftp_root_read"] = "tmp/files"
    centre = Centre(config, centre_config)
    centre.process_files(add_to_dart=False)

    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)

    date_time = datetime(year=2020, month=4, day=16, hour=14, minute=30, second=40)

    # Testing file where values contain with whitespace
    assert (
        samples_collection.count_documents(
            {
                FIELD_ROOT_SAMPLE_ID: "3",
                FIELD_VIRAL_PREP_ID: "1",
                FIELD_RNA_ID: "AP456_B09",
                FIELD_RNA_PCR_ID: "CF06CR9G_B03",
                FIELD_RESULT: "Negative",
                FIELD_SOURCE: "Test Centre",
                FIELD_DATE_TESTED: date_time,
            }
        )
        == 1
    )
    assert samples_collection.count_documents({FIELD_RNA_ID: "AP456_B08"}) == 1
    assert (
        imports_collection.count_documents({"csv_file_used": "TEST_sanger_report_200518_2208_with_whitespace.csv"}) == 1
    )


# ----- tests for class CentreFile -----


@pytest.mark.parametrize(
    "filename, expected_value",
    [
        [UNCONSOLIDATED_SURVEILLANCE_FILENAME, False],
        [CONSOLIDATED_EAGLE_FILENAME, True],
        [CONSOLIDATED_SURVEILLANCE_FILENAME, True],
    ],
)
def test_can_identify_consolidated_file(config, filename, expected_value):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile(filename, centre)

    assert centre_file.is_consolidated is expected_value


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
    with patch.dict(config.CENTRES[0], {CENTRE_KEY_BACKUPS_FOLDER: tmpdir.realpath()}):
        tmpdir.mkdir(SUCCESSES_DIR)

        list_files = create_checksum_files_for(
            f"{config.CENTRES[0][CENTRE_KEY_BACKUPS_FOLDER]}/successes/",
            "AP_sanger_report_200503_2338.csv",
            ["adfsadf", "asdf"],
            "200601_1414",
        )

        try:
            centre = Centre(config, config.CENTRES[0])
            centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)

            assert centre_file.checksum_match(SUCCESSES_DIR) is False
        finally:
            for tmpfile_for_list in list_files:
                os.remove(tmpfile_for_list)


def test_checksum_match(config, tmpdir):
    with patch.dict(config.CENTRES[0], {CENTRE_KEY_BACKUPS_FOLDER: tmpdir.realpath()}):
        tmpdir.mkdir(SUCCESSES_DIR)

        list_files = create_checksum_files_for(
            f"{config.CENTRES[0][CENTRE_KEY_BACKUPS_FOLDER]}/successes/",
            "AP_sanger_report_200503_2338.csv",
            ["adfsadf", "d204bd7747d9ad505eee901830448578"],
            "200601_1414",
        )

        try:
            centre = Centre(config, config.CENTRES[0])
            centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)
            assert centre_file.checksum_match(SUCCESSES_DIR) is True
        finally:
            for tmpfile_for_list in list_files:
                os.remove(tmpfile_for_list)


# tests for validating row structure
def test_row_required_fields_present_fail(config: Config, centre_file: CentreFile) -> None:
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
    test_centre = config.CENTRES[0]
    centre = Centre(config, test_centre)
    centre_file = CentreFile("some file", centre)

    barcode_field = test_centre[CENTRE_KEY_BARCODE_FIELD]
    barcode_regex = test_centre[CENTRE_KEY_BARCODE_REGEX]

    # typical format
    typical = {"RNA ID": "AP-abc-12345678_H01"}
    assert centre_file.extract_plate_barcode_and_coordinate(typical, 0, barcode_field, barcode_regex) == (
        "AP-abc-12345678",
        "H01",
    )

    # coordinate zero
    coord_zero = {"RNA ID": "AP-abc-12345678_A00"}
    assert centre_file.extract_plate_barcode_and_coordinate(coord_zero, 0, barcode_field, barcode_regex) == (
        "AP-abc-12345678",
        "A00",
    )

    # invalid coordinate format
    invalid_coord = {"RNA ID": "AP-abc-12345678_HH0"}
    assert centre_file.extract_plate_barcode_and_coordinate(invalid_coord, 0, barcode_field, barcode_regex) == (
        "",
        "",
    )

    # missing underscore between plate barcode and coordinate
    missing = {"RNA ID": "AP-abc-12345678H0"}
    assert centre_file.extract_plate_barcode_and_coordinate(missing, 0, barcode_field, barcode_regex) == (
        "",
        "",
    )

    # shorter plate barcode
    short = {"RNA ID": "DN1234567_H01"}
    assert centre_file.extract_plate_barcode_and_coordinate(short, 0, barcode_field, barcode_regex) == (
        "DN1234567",
        "H01",
    )

    # from RT
    rt = {"RNA ID": "`AP-abc-00020028_C11"}
    assert centre_file.extract_plate_barcode_and_coordinate(rt, 0, barcode_field, barcode_regex) == (
        "AP-abc-00020028",
        "C11",
    )

    # white space and garbage characters
    garbage = {"RNA ID": "   `£$%^&AP-abc-12345678_H01(*&^%$ ` `"}
    assert centre_file.extract_plate_barcode_and_coordinate(garbage, 0, barcode_field, barcode_regex) == (
        "AP-abc-12345678",
        "H01",
    )

    # lowercase coordinates
    lower_coord = {"RNA ID": "AP-abc-12345678_h01"}
    assert centre_file.extract_plate_barcode_and_coordinate(lower_coord, 0, barcode_field, barcode_regex) == (
        "",
        "",
    )

    # unpadded coordinates
    lower_coord = {"RNA ID": "AP-abc-12345678_A2"}
    assert centre_file.extract_plate_barcode_and_coordinate(lower_coord, 0, barcode_field, barcode_regex) == (
        "AP-abc-12345678",
        "A02",
    )


def test_parse_and_format_file_rows(config, freezer):
    """Tests for parsing and formatting the csv file rows"""
    now = datetime.now()
    test_uuid = uuid.uuid4()
    centre_file = centre_file_with_mocked_filtered_positive_identifier(config, "some file")
    with patch("crawler.file_processing.uuid.uuid4", return_value=test_uuid):
        extra_fields_added = [
            {
                FIELD_ROOT_SAMPLE_ID: "1",
                FIELD_RNA_ID: "RNA_0043_H09",
                FIELD_PLATE_BARCODE: "RNA_0043",
                FIELD_SOURCE: "Alderley",
                FIELD_COORDINATE: "H09",
                FIELD_LINE_NUMBER: 2,
                FIELD_RESULT: "Positive",
                FIELD_FILE_NAME: "some file",
                FIELD_FILE_NAME_DATE: None,
                FIELD_CREATED_AT: now,
                FIELD_UPDATED_AT: now,
                FIELD_LAB_ID: "",
                FIELD_FILTERED_POSITIVE: True,
                FIELD_FILTERED_POSITIVE_VERSION: "v2.3",
                FIELD_FILTERED_POSITIVE_TIMESTAMP: now,
                FIELD_LH_SAMPLE_UUID: str(test_uuid),
                FIELD_DATE_TESTED: None,
            }
        ]

        with StringIO() as fake_csv:
            fake_csv.write(f"{FIELD_ROOT_SAMPLE_ID},{FIELD_RNA_ID},{FIELD_RESULT},{FIELD_LAB_ID},{FIELD_DATE_TESTED}\n")
            fake_csv.write("1,RNA_0043_H09,Positive,,\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)
            assert augmented_data == extra_fields_added
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0


def test_parse_and_format_file_rows_with_invalid_rna_id(config):
    centre_file = centre_file_with_mocked_filtered_positive_identifier(config, "some file")
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
            f"{FIELD_ROOT_SAMPLE_ID},{FIELD_RNA_ID},{FIELD_RESULT},{FIELD_DATE_TESTED},"
            f"{FIELD_LAB_ID},{FIELD_CH1_TARGET},{FIELD_CH1_RESULT},{FIELD_CH1_CQ},extra_col_1,"
            "extra_col_2,extra_col_3\n"
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
            "CH1-Target": "ORF1ab",
            "CH1-Result": "Positive",
            "CH1-Cq": "23.12345678",
        }

        assert centre_file.filtered_row(next(csv_to_test_reader), 2) == expected_row
        assert centre_file.logging_collection.aggregator_types["TYPE 13"].count_errors == 1
        # N.B. Type 13 is a WARNING type and not counted as an error or critical
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0


def test_extract_channel_fields(centre_file: CentreFile) -> None:
    with StringIO() as csv_weird_channels:
        csv_weird_channels.write(
            f"{FIELD_ROOT_SAMPLE_ID},{FIELD_RNA_ID},{FIELD_RESULT},{FIELD_DATE_TESTED},{FIELD_LAB_ID},"
            "CH 1 - Target,CH 1 - Result,CH 1 - Cq,CH2_Target,CH2_Result,CH2_Cq,CH3-Target,CH3-Result,CH3-Cq,\n"
        )
        csv_weird_channels.write(
            "1,RNA_0043,Positive,today,AP,ORF1ab,Positive,21.433,ORF2ab,Negative,22.433,ORF3ab,Positive,23.433,\n"
        )
        csv_weird_channels.seek(0)

        csv_to_test_reader = DictReader(csv_weird_channels)

        modified_row: ModifiedRow = {
            FIELD_ROOT_SAMPLE_ID: "1",
            FIELD_RNA_ID: "RNA_0043",
            FIELD_RESULT: "Positive",
            FIELD_DATE_TESTED: "today",
            FIELD_LAB_ID: "AP",
        }

        expected_modified_row = {
            FIELD_ROOT_SAMPLE_ID: "1",
            FIELD_RNA_ID: "RNA_0043",
            FIELD_RESULT: "Positive",
            FIELD_DATE_TESTED: "today",
            FIELD_LAB_ID: "AP",
            FIELD_CH1_TARGET: "ORF1ab",
            FIELD_CH1_RESULT: "Positive",
            FIELD_CH1_CQ: "21.433",
            FIELD_CH2_TARGET: "ORF2ab",
            FIELD_CH2_RESULT: "Negative",
            FIELD_CH2_CQ: "22.433",
            FIELD_CH3_TARGET: "ORF3ab",
            FIELD_CH3_RESULT: "Positive",
            FIELD_CH3_CQ: "23.433",
        }

        seen_headers = [FIELD_ROOT_SAMPLE_ID, FIELD_RNA_ID, FIELD_RESULT, FIELD_DATE_TESTED, FIELD_LAB_ID]

        expected_seen_headers = seen_headers + [
            "CH 1 - Target",
            "CH 1 - Result",
            "CH 1 - Cq",
            "CH2_Target",
            "CH2_Result",
            "CH2_Cq",
            "CH3-Target",
            "CH3-Result",
            "CH3-Cq",
        ]

        seen_headers_to_test, modified_row_to_test = centre_file.extract_channel_fields(
            seen_headers, next(csv_to_test_reader), modified_row
        )

        assert seen_headers_to_test == expected_seen_headers
        assert modified_row_to_test == expected_modified_row


def test_filtered_row_with_blank_lab_id(config):
    # check when flag set in config it adds default lab id
    try:
        config.ADD_LAB_ID = True
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some_file.csv", centre)

        with StringIO() as fake_csv_without_lab_id:
            fake_csv_without_lab_id.write(f"{FIELD_ROOT_SAMPLE_ID},{FIELD_RNA_ID},{FIELD_RESULT},{FIELD_DATE_TESTED}\n")
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

        with StringIO() as fake_csv_with_lab_id:
            fake_csv_with_lab_id.write(
                f"{FIELD_ROOT_SAMPLE_ID},{FIELD_RNA_ID},{FIELD_RESULT},{FIELD_DATE_TESTED},{FIELD_LAB_ID}\n"
            )
            fake_csv_with_lab_id.write("1,RNA_0043,Positive,today,RealLabID\n")
            fake_csv_with_lab_id.seek(0)

            csv_to_test_reader = DictReader(fake_csv_with_lab_id)

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
            ",".join(
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
                    FIELD_CH4_CQ,
                )
            )
            + "\n"
        )
        fake_csv_with_ct_columns.write(
            ",".join(
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
                    "24.98589115\n",
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


def test_parse_and_format_file_rows_to_add_file_details(config, freezer):
    now = datetime.now()
    test_uuid = uuid.uuid4()
    fake_file_name = "fake_200507_1340.csv"
    centre_file = centre_file_with_mocked_filtered_positive_identifier(config, fake_file_name)
    with patch("crawler.file_processing.uuid.uuid4", return_value=test_uuid):
        extra_fields_added = [
            {
                "Root Sample ID": "1",
                "RNA ID": "RNA_0043_H09",
                "plate_barcode": "RNA_0043",
                "source": "Alderley",
                "coordinate": "H09",
                "line_number": 2,
                "file_name": fake_file_name,
                "file_name_date": datetime(2020, 5, 7, 13, 40),
                "created_at": now,
                "updated_at": now,
                "Result": "Positive",
                FIELD_LAB_ID: "",
                "filtered_positive": True,
                "filtered_positive_version": "v2.3",
                "filtered_positive_timestamp": now,
                "lh_sample_uuid": str(test_uuid),
                FIELD_DATE_TESTED: None,
            },
            {
                "Root Sample ID": "2",
                "RNA ID": "RNA_0043_B08",
                "plate_barcode": "RNA_0043",
                "source": "Alderley",
                "coordinate": "B08",
                "line_number": 3,
                "file_name": fake_file_name,
                "file_name_date": datetime(2020, 5, 7, 13, 40),
                "created_at": now,
                "updated_at": now,
                "Result": "Negative",
                FIELD_LAB_ID: "",
                "filtered_positive": True,
                "filtered_positive_version": "v2.3",
                "filtered_positive_timestamp": now,
                "lh_sample_uuid": str(test_uuid),
                FIELD_DATE_TESTED: None,
            },
        ]

        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,Date Tested\n")
            fake_csv.write("1,RNA_0043_H09,Positive,\n")
            fake_csv.write("2,RNA_0043_B08,Negative,\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)

            assert augmented_data == extra_fields_added
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0


def test_parse_and_format_file_rows_detects_duplicates(config, freezer):
    now = datetime.now()
    test_uuid = uuid.uuid4()
    fake_file_name = "fake_200507_1340.csv"
    centre_file = centre_file_with_mocked_filtered_positive_identifier(config, fake_file_name)
    with patch("crawler.file_processing.uuid.uuid4", return_value=test_uuid):
        extra_fields_added = [
            {
                "Root Sample ID": "1",
                "RNA ID": "RNA_0043_H09",
                "plate_barcode": "RNA_0043",
                "source": "Alderley",
                "coordinate": "H09",
                "line_number": 2,
                "file_name": fake_file_name,
                "file_name_date": datetime(2020, 5, 7, 13, 40),
                "created_at": now,
                "updated_at": now,
                "Result": "Positive",
                "Lab ID": "Val",
                "filtered_positive": True,
                "filtered_positive_version": "v2.3",
                "filtered_positive_timestamp": now,
                "lh_sample_uuid": str(test_uuid),
                FIELD_DATE_TESTED: None,
            },
        ]

        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,Date Tested\n")
            fake_csv.write("1,RNA_0043_H09,Positive,Val,\n")
            fake_csv.write("1,RNA_0043_H09,Positive,Val,\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.parse_and_format_file_rows(csv_to_test_reader)
            assert augmented_data == extra_fields_added

            assert centre_file.logging_collection.aggregator_types["TYPE 5"].count_errors == 1
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0


def test_parse_and_format_file_rows_can_parse_empty_root_sample_id(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with StringIO() as fake_csv:
        fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID\n")

        # where row has valid value - should pass
        fake_csv.write("1,RNA_0043_H06,Positive,Val\n")

        # where row has empty - should error
        fake_csv.write(",RNA_0043_H08,limit of detection,Val\n")

        # where row has Empty - should ignore
        fake_csv.write("Empty  ,RNA_0043_H09,Void,Val\n")

        # where row has empty - should ignore
        fake_csv.write("empty  ,RNA_0043_H09,Void,Val\n")

        # another valid sample
        fake_csv.write("2,RNA_0043_H10,Positive,Val\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)
        read_rows = centre_file.parse_and_format_file_rows(csv_to_test_reader)

        assert len(read_rows) == 2
        assert read_rows[0][FIELD_ROOT_SAMPLE_ID] == "1"
        assert read_rows[1][FIELD_ROOT_SAMPLE_ID] == "2"

        # should not have any errors (Ignore empty and no values)
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0


def test_where_result_has_unexpected_value(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

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
        centre_file.parse_and_format_file_rows(csv_to_test_reader)

        # should create a specific error type for the row
        assert centre_file.logging_collection.aggregator_types["TYPE 16"].count_errors == 1
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1


def test_where_ct_channel_fields_can_have_unknown(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with StringIO() as fake_csv:
        fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,CH2-Target,CH2-Result\n")

        # where row has valid value - should pass
        fake_csv.write("1,RNA_0043_H09,Positive,Val,Unknown,Unknown\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)
        read = centre_file.parse_and_format_file_rows(csv_to_test_reader)

        # should create a specific error type for the row
        assert len(read) == 1
        assert read[0][FIELD_ROOT_SAMPLE_ID] == "1"
        assert read[0][FIELD_CH2_TARGET] is None
        assert read[0][FIELD_CH2_RESULT] is None
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0


def test_parse_and_format_file_rows_where_predefined_unknown_headers_are_not_stored_or_logged_in_imports_record(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with StringIO() as fake_csv:
        fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,PickResult,testKit\n")

        # where row has valid value - should pass
        fake_csv.write("1,RNA_0043_H09,Positive,Val,Good pick,Mytest\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)
        read = centre_file.parse_and_format_file_rows(csv_to_test_reader)

        # should create a specific error type for the row
        assert len(read) == 1
        assert read[0][FIELD_ROOT_SAMPLE_ID] == "1"
        assert not ("PickResult" in read[0])
        assert not ("testKit" in read[0])

        assert centre_file.logging_collection.aggregator_types["TYPE 13"].count_errors == 0
        assert len(centre_file.logging_collection.get_messages_for_import()) == 0


def test_parse_and_format_file_rows_where_not_predefined_unknown_headers_are_stored_and_they_are_in_the_imports_record(
    config,
):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with StringIO() as fake_csv:
        fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,NewHeader\n")

        # where row has valid value - should pass
        fake_csv.write("1,RNA_0043_H09,Positive,Val,Good pick\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)
        read = centre_file.parse_and_format_file_rows(csv_to_test_reader)

        # should create a specific error type for the row
        assert len(read) == 1
        assert read[0][FIELD_ROOT_SAMPLE_ID] == "1"
        assert not ("NewHeader" in read[0])
        assert centre_file.logging_collection.aggregator_types["TYPE 13"].count_errors == 1


def test_where_ct_channel_target_has_unexpected_value(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    with StringIO() as fake_csv:
        fake_csv.write("Root Sample ID,RNA ID,Result,Lab ID,CH1-Target\n")

        # where row has valid value - should pass
        fake_csv.write("1,RNA_0043_H09,Positive,Val,S gene\n")
        fake_csv.write("1,RNA_0043_H09,Positive,Val,RNaseP\n")
        fake_csv.write("1,RNA_0043_H09,Positive,Val,IEC\n")

        # where row is empty - should pass
        fake_csv.write("2,RNA_0043_H10,Positive,Val,\n")

        # where row has invalid value - should error
        fake_csv.write("2,RNA_0043_H11,Positive,Val,NotATarget\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)
        centre_file.parse_and_format_file_rows(csv_to_test_reader)

        # should create a specific error type for the row
        assert centre_file.logging_collection.aggregator_types["TYPE 17"].count_errors == 1
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1


def test_where_ct_channel_result_has_unexpected_value(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

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
        centre_file.parse_and_format_file_rows(csv_to_test_reader)

        # should create a specific error type for the row
        assert centre_file.logging_collection.aggregator_types["TYPE 18"].count_errors == 1
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1


def test_changes_ct_channel_cq_value_data_type(config: Config, centre_file: CentreFile) -> None:
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
        centre_file.parse_and_format_file_rows(csv_to_test_reader)

        # should create a specific error type for the row
        assert centre_file.logging_collection.aggregator_types["TYPE 19"].count_errors == 1
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1


def test_is_within_cq_range():
    assert CentreFile.is_within_cq_range(Decimal("0.0"), Decimal("100.0"), Decimal128("0.0")) is True
    assert CentreFile.is_within_cq_range(Decimal("0.0"), Decimal("100.0"), Decimal128("100.0")) is True
    assert CentreFile.is_within_cq_range(Decimal("0.0"), Decimal("100.0"), Decimal128("27.019291283")) is True

    assert CentreFile.is_within_cq_range(Decimal("0.0"), Decimal("100.0"), Decimal128("-0.00000001")) is False
    assert CentreFile.is_within_cq_range(Decimal("0.0"), Decimal("100.0"), Decimal128("100.00000001")) is False

    assert CentreFile.is_within_cq_range(Decimal("NaN"), Decimal("100.0"), Decimal128("50.0")) is False
    assert CentreFile.is_within_cq_range(Decimal("0.0"), Decimal("NaN"), Decimal128("50.0")) is False
    assert CentreFile.is_within_cq_range(Decimal("0.0"), Decimal("100.0"), Decimal128("NaN")) is False


def test_where_ct_channel_cq_value_is_not_within_range(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

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
        centre_file.parse_and_format_file_rows(csv_to_test_reader)

        # should create a specific error type for the row
        assert centre_file.logging_collection.aggregator_types["TYPE 20"].count_errors == 2
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 2


def test_where_positive_result_does_not_align_with_ct_channel_results(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

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
        centre_file.parse_and_format_file_rows(csv_to_test_reader)

        # should create a specific error type for the row
        assert centre_file.logging_collection.aggregator_types["TYPE 21"].count_errors == 1
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1


@pytest.mark.parametrize(
    "filename, expected_value",
    [
        [UNCONSOLIDATED_SURVEILLANCE_FILENAME, True],
        [CONSOLIDATED_EAGLE_FILENAME, False],
        [CONSOLIDATED_SURVEILLANCE_FILENAME, False],
    ],
)
def test_can_identify_unconsolidated_surveillance_file(config, testing_centres, filename, expected_value):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile(filename, centre)
    assert centre_file.is_unconsolidated_surveillance_file() is expected_value


def test_log_unprocessed_takes_needed_actions(mongo_database, config, testing_files_for_process):
    _, mongo_database = mongo_database

    centre_config = config.CENTRES[0]
    centre_config["sftp_root_read"] = "tmp/files"
    centre = Centre(config, centre_config)
    centre_file = CentreFile(UNCONSOLIDATED_SURVEILLANCE_FILENAME, centre)
    centre_file.log_unprocessed()

    # Assert TYPE 34 log messages were created
    log_messages = centre_file.logging_collection.get_messages_for_import()
    assert len(log_messages) == 2
    assert all("TYPE 34" in err for err in log_messages)

    # Assert that files were stored in backups as errors
    errors_path = os.path.join(centre_config[CENTRE_KEY_BACKUPS_FOLDER], ERRORS_DIR)
    assert len(os.listdir(errors_path)) == 1

    # Assert that an import record was created
    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    assert imports_collection.count_documents({}) == 1


def test_remove_bom(centre_file):
    with StringIO() as fake_csv:
        # construct a bytes object containing a byte order mark (BOM)
        header_with_bom = b"\xef\xbb\xbfRoot Sample ID"
        bom_as_utf8_string = header_with_bom.decode("utf-8")

        fake_csv.write(f"{bom_as_utf8_string},RNA ID\n")
        fake_csv.write("1,RNA_0043_\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)
        assert csv_to_test_reader.fieldnames != ["Root Sample ID", "RNA ID"]

        centre_file.remove_bom(csv_to_test_reader)
        assert csv_to_test_reader.fieldnames == ["Root Sample ID", "RNA ID"]


def test_correct_headers_match(centre_file):
    with StringIO() as fake_csv:
        fake_csv.write(" Root Sample  ,RNA ID\n")
        fake_csv.write("1,RNA_0043_\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)
        assert csv_to_test_reader.fieldnames == [" Root Sample  ", "RNA ID"]

        centre_file.correct_headers(csv_to_test_reader)
        # matched regex so was corrected
        assert csv_to_test_reader.fieldnames == ["Root Sample ID", "RNA ID"]


def test_correct_headers_no_match(centre_file):
    with StringIO() as fake_csv:
        fake_csv.write("Root Sample Wrong Name,RNA ID\n")
        fake_csv.write("1,RNA_0043_\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)
        assert csv_to_test_reader.fieldnames == ["Root Sample Wrong Name", "RNA ID"]

        centre_file.correct_headers(csv_to_test_reader)
        # didn't match regex so wasn't corrected
        assert csv_to_test_reader.fieldnames == ["Root Sample Wrong Name", "RNA ID"]


def test_correct_headers_already_correct(centre_file):
    with StringIO() as fake_csv:
        fake_csv.write("Root Sample ID,RNA ID\n")
        fake_csv.write("1,RNA_0043_\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)
        assert csv_to_test_reader.fieldnames == ["Root Sample ID", "RNA ID"]

        centre_file.correct_headers(csv_to_test_reader)
        # was already correct, unaffected
        assert csv_to_test_reader.fieldnames == ["Root Sample ID", "RNA ID"]


def test_check_for_required_headers_empty_file(centre_file):
    # empty file
    with StringIO() as fake_csv:
        csv_to_test_reader = DictReader(fake_csv)
        assert centre_file.check_for_required_headers(csv_to_test_reader) is False
        assert centre_file.logging_collection.aggregator_types["TYPE 2"].count_errors == 1
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1


def test_check_for_required_headers_with_incorrect_headers(centre_file):
    # incorrect_headers
    with StringIO() as fake_csv:
        fake_csv.write("id,RNA ID\n")
        fake_csv.write("1,RNA_0043_\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)

        assert centre_file.check_for_required_headers(csv_to_test_reader) is False
        assert centre_file.logging_collection.aggregator_types["TYPE 2"].count_errors == 1
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1


def test_check_for_required_headers_with_valid_headers(centre_file):
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


def test_check_for_required_headers_with_missing_lab_id_and_lab_id_false(centre_file: CentreFile) -> None:
    # file with missing Lab ID header and add lab id false (default)
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


def test_check_for_required_headers_with_missing_lab_id_and_lab_id_true(config):
    # file with missing Lab ID header and add lab id true
    # we need to use a try, finally block here since the config fixture returns the real config object, not a copy
    # so we need to reset any config we change
    # TODO: find a way to improve this
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
    with patch.dict(config.CENTRES[0], {CENTRE_KEY_BACKUPS_FOLDER: tmpdir.realpath()}):
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
    with patch.dict(config.CENTRES[0], {CENTRE_KEY_BACKUPS_FOLDER: tmpdir.realpath()}):
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
    file_name_date = centre_file.file_name_date()

    assert file_name_date is not None
    assert file_name_date.year == 2020
    assert file_name_date.month == 5
    assert file_name_date.day == 3
    assert file_name_date.hour == 23
    assert file_name_date.minute == 38

    centre_file = CentreFile("AP_sanger_report_200503_2338 (2).csv", centre)
    assert centre_file.file_name_date() is None


def test_insert_samples_from_docs_into_mlwh(
    config: Config, mlwh_connection: CMySQLConnection, centre_file: CentreFile
) -> None:
    """Tests for inserting docs into mlwh using rows with and without ct columns"""
    date_tested_1 = datetime(2020, 4, 23, 14, 40, 0)
    date_tested_2 = datetime(2020, 4, 23, 14, 41, 0)
    filtered_positive_timestamp = datetime(2020, 4, 23, 14, 41, 0)

    docs: List[SampleDoc] = [
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000004",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
            FIELD_RNA_ID: "TC-rna-00000029_H11",
            FIELD_PLATE_BARCODE: "TC-rna-00000029",
            FIELD_COORDINATE: "H11",
            FIELD_RESULT: "Negative",
            FIELD_DATE_TESTED: datetime(2020, 4, 23, 14, 40, 0),
            FIELD_SOURCE: "Test Centre",
            FIELD_LAB_ID: "TC",
            FIELD_MUST_SEQUENCE: False,
            FIELD_PREFERENTIALLY_SEQUENCE: True,
        },
        {
            "_id": ObjectId("5f562d9931d9959b92544729"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000005",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABD",
            FIELD_RNA_ID: "TC-rna-00000029_H12",
            FIELD_PLATE_BARCODE: "TC-rna-00000029",
            FIELD_COORDINATE: "H12",
            FIELD_RESULT: RESULT_VALUE_POSITIVE,
            FIELD_DATE_TESTED: date_tested_2,
            FIELD_SOURCE: "Test Centre",
            FIELD_LAB_ID: "TC",
            FIELD_CH1_TARGET: "ORF1ab",
            FIELD_CH1_RESULT: RESULT_VALUE_POSITIVE,
            FIELD_CH1_CQ: Decimal128("21.28726211"),
            FIELD_CH2_TARGET: "N gene",
            FIELD_CH2_RESULT: RESULT_VALUE_POSITIVE,
            FIELD_CH2_CQ: Decimal128("18.12736661"),
            FIELD_CH3_TARGET: "S gene",
            FIELD_CH3_RESULT: RESULT_VALUE_POSITIVE,
            FIELD_CH3_CQ: Decimal128("22.63616273"),
            FIELD_CH4_TARGET: "MS2",
            FIELD_CH4_RESULT: RESULT_VALUE_POSITIVE,
            FIELD_CH4_CQ: Decimal128("26.25125612"),
            FIELD_FILTERED_POSITIVE: True,
            FIELD_FILTERED_POSITIVE_VERSION: "v2.3",
            FIELD_FILTERED_POSITIVE_TIMESTAMP: filtered_positive_timestamp,
            FIELD_MUST_SEQUENCE: True,
            FIELD_PREFERENTIALLY_SEQUENCE: False,
        },
    ]

    result = centre_file.insert_samples_from_docs_into_mlwh(docs)

    assert result is True
    error_count = centre_file.logging_collection.get_count_of_all_errors_and_criticals()
    error_messages = centre_file.logging_collection.get_aggregate_messages()
    assert (
        error_count == 0
    ), f"Should not be any errors. Actual number errors: {error_count}. Error details: {error_messages}"

    cursor = mlwh_connection.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME}")
    rows = cursor.fetchall()
    cursor.close()

    assert rows[0][MLWH_MONGODB_ID] == "5f562d9931d9959b92544728"
    assert rows[0][MLWH_ROOT_SAMPLE_ID] == "ABC00000004"
    assert rows[0][MLWH_COG_UK_ID] == "TEST-123ABC"
    assert rows[0][MLWH_RNA_ID] == "TC-rna-00000029_H11"
    assert rows[0][MLWH_PLATE_BARCODE] == "TC-rna-00000029"
    assert rows[0][MLWH_COORDINATE] == "H11"
    assert rows[0][MLWH_RESULT] == "Negative"
    assert rows[0][MLWH_DATE_TESTED] == date_tested_1
    assert rows[0][MLWH_SOURCE] == "Test Centre"
    assert rows[0][MLWH_LAB_ID] == "TC"
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
    assert rows[0][MLWH_MUST_SEQUENCE] == 0
    assert rows[0][MLWH_PREFERENTIALLY_SEQUENCE] == 1

    assert rows[1][MLWH_MONGODB_ID] == "5f562d9931d9959b92544729"
    assert rows[1][MLWH_ROOT_SAMPLE_ID] == "ABC00000005"
    assert rows[1][MLWH_COG_UK_ID] == "TEST-123ABD"
    assert rows[1][MLWH_RNA_ID] == "TC-rna-00000029_H12"
    assert rows[1][MLWH_PLATE_BARCODE] == "TC-rna-00000029"
    assert rows[1][MLWH_COORDINATE] == "H12"
    assert rows[1][MLWH_RESULT] == RESULT_VALUE_POSITIVE
    assert rows[1][MLWH_DATE_TESTED] == date_tested_2
    assert rows[1][MLWH_SOURCE] == "Test Centre"
    assert rows[1][MLWH_LAB_ID] == "TC"
    assert rows[1][MLWH_CH1_TARGET] == "ORF1ab"
    assert rows[1][MLWH_CH1_RESULT] == RESULT_VALUE_POSITIVE
    assert rows[1][MLWH_CH1_CQ] == Decimal("21.28726211")
    assert rows[1][MLWH_CH2_TARGET] == "N gene"
    assert rows[1][MLWH_CH2_RESULT] == RESULT_VALUE_POSITIVE
    assert rows[1][MLWH_CH2_CQ] == Decimal("18.12736661")
    assert rows[1][MLWH_CH3_TARGET] == "S gene"
    assert rows[1][MLWH_CH3_RESULT] == RESULT_VALUE_POSITIVE
    assert rows[1][MLWH_CH3_CQ] == Decimal("22.63616273")
    assert rows[1][MLWH_CH4_TARGET] == "MS2"
    assert rows[1][MLWH_CH4_RESULT] == RESULT_VALUE_POSITIVE
    assert rows[1][MLWH_CH4_CQ] == Decimal("26.25125612")
    assert rows[1][MLWH_FILTERED_POSITIVE] == 1
    assert rows[1][MLWH_FILTERED_POSITIVE_VERSION] == "v2.3"
    assert rows[1][MLWH_FILTERED_POSITIVE_TIMESTAMP] == filtered_positive_timestamp
    assert rows[1][MLWH_CREATED_AT] is not None
    assert rows[1][MLWH_UPDATED_AT] is not None
    assert rows[1][MLWH_MUST_SEQUENCE] == 1
    assert rows[1][MLWH_PREFERENTIALLY_SEQUENCE] == 0


def test_insert_samples_from_docs_into_mlwh_date_tested_missing(config, mlwh_connection):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    docs: List[SampleDoc] = [
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000004",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
            FIELD_RNA_ID: "TC-rna-00000029_H11",
            FIELD_PLATE_BARCODE: "TC-rna-00000029",
            FIELD_COORDINATE: "H11",
            FIELD_RESULT: "Negative",
            FIELD_SOURCE: "Test Centre",
            FIELD_LAB_ID: "TC",
        }
    ]

    result = centre_file.insert_samples_from_docs_into_mlwh(docs)

    assert result is True
    error_count = centre_file.logging_collection.get_count_of_all_errors_and_criticals()
    error_messages = centre_file.logging_collection.get_aggregate_messages()
    assert (
        error_count == 0
    ), f"Should not be any errors. Actual number errors: {error_count}. Error details: {error_messages}"

    cursor = mlwh_connection.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME}")
    rows = cursor.fetchall()
    cursor.close()

    assert rows[0][MLWH_DATE_TESTED] is None


def test_insert_samples_from_docs_into_mlwh_date_tested_none(
    config: Config, mlwh_connection: CMySQLConnection, centre_file: CentreFile
) -> None:
    docs: List[SampleDoc] = [
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000004",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
            FIELD_RNA_ID: "TC-rna-00000029_H11",
            FIELD_PLATE_BARCODE: "TC-rna-00000029",
            FIELD_COORDINATE: "H11",
            FIELD_RESULT: "Negative",
            FIELD_DATE_TESTED: None,
            FIELD_SOURCE: "Test Centre",
            FIELD_LAB_ID: "TC",
        }
    ]

    result = centre_file.insert_samples_from_docs_into_mlwh(docs)

    assert result is True
    error_count = centre_file.logging_collection.get_count_of_all_errors_and_criticals()
    error_messages = centre_file.logging_collection.get_aggregate_messages()
    assert (
        error_count == 0
    ), f"Should not be any errors. Actual number errors: {error_count}. Error details: {error_messages}"

    cursor = mlwh_connection.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME}")
    rows = cursor.fetchall()
    cursor.close()

    assert rows[0][MLWH_DATE_TESTED] is None


def test_insert_samples_from_docs_into_mlwh_returns_false_none_connection(config, mlwh_connection):
    with patch("crawler.db.mysql.create_mysql_connection", return_value=None):
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)

        docs: List[SampleDoc] = [
            {
                "_id": ObjectId("5f562d9931d9959b92544728"),
                FIELD_ROOT_SAMPLE_ID: "ABC00000004",
                FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
                FIELD_RNA_ID: "TC-rna-00000029_H11",
                FIELD_PLATE_BARCODE: "TC-rna-00000029",
                FIELD_COORDINATE: "H11",
                FIELD_RESULT: "Negative",
                FIELD_DATE_TESTED: "",
                FIELD_SOURCE: "Test Centre",
                FIELD_LAB_ID: "TC",
            }
        ]

        result = centre_file.insert_samples_from_docs_into_mlwh(docs)
        assert result is False


def test_insert_samples_from_docs_into_mlwh_returns_false_not_connected(config, mlwh_connection):
    with patch("crawler.db.mysql.create_mysql_connection") as mysql_conn:
        mysql_conn().is_connected.return_value = False
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)

        docs: List[SampleDoc] = [
            {
                "_id": ObjectId("5f562d9931d9959b92544728"),
                FIELD_ROOT_SAMPLE_ID: "ABC00000004",
                FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
                FIELD_RNA_ID: "TC-rna-00000029_H11",
                FIELD_PLATE_BARCODE: "TC-rna-00000029",
                FIELD_COORDINATE: "H11",
                FIELD_RESULT: "Negative",
                FIELD_DATE_TESTED: "",
                FIELD_SOURCE: "Test Centre",
                FIELD_LAB_ID: "TC",
            }
        ]

        result = centre_file.insert_samples_from_docs_into_mlwh(docs)
        assert result is False


def test_insert_samples_from_docs_into_mlwh_returns_failure_executing(config, mlwh_connection):
    with patch("crawler.db.mysql.create_mysql_connection"):
        with patch("crawler.db.mysql.run_mysql_executemany_query", side_effect=MockedError("Boom!")):
            centre = Centre(config, config.CENTRES[0])
            centre_file = CentreFile("some file", centre)

            docs: List[SampleDoc] = [
                {
                    "_id": ObjectId("5f562d9931d9959b92544728"),
                    FIELD_ROOT_SAMPLE_ID: "ABC00000004",
                    FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
                    FIELD_RNA_ID: "TC-rna-00000029_H11",
                    FIELD_PLATE_BARCODE: "TC-rna-00000029",
                    FIELD_COORDINATE: "H11",
                    FIELD_RESULT: "Negative",
                    FIELD_DATE_TESTED: "",
                    FIELD_SOURCE: "Test Centre",
                    FIELD_LAB_ID: "TC",
                }
            ]

            result = centre_file.insert_samples_from_docs_into_mlwh(docs)
            assert result is False


# tests for inserting docs into DART
def test_insert_plates_and_wells_from_docs_into_dart_none_connection(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    with patch("crawler.file_processing.create_dart_sql_server_conn", return_value=None):
        centre_file.insert_plates_and_wells_from_docs_into_dart([])

        # logs error on failing to initialise the SQL server connection
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
        assert centre_file.logging_collection.aggregator_types["TYPE 24"].count_errors == 1


def test_insert_plates_and_wells_from_docs_into_dart_failed_cursor(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    with patch("crawler.file_processing.create_dart_sql_server_conn") as mock_conn:
        mock_conn().cursor = MagicMock(side_effect=MockedError("Boom!"))
        centre_file.insert_plates_and_wells_from_docs_into_dart([])

        # logs error on failing to initialise the SQL server cursor
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
        assert centre_file.logging_collection.aggregator_types["TYPE 23"].count_errors == 1
        mock_conn().close.assert_called_once()


def test_insert_plates_and_wells_from_docs_into_dart_failure_adding_new_plate(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)
    docs_to_insert: List[SampleDoc] = [
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000004",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
            FIELD_RNA_ID: "TC-rna-00000029_H11",
            FIELD_PLATE_BARCODE: "TC-rna-00000029",
            FIELD_LAB_ID: "AP",
            FIELD_COORDINATE: "H11",
            FIELD_RESULT: RESULT_VALUE_POSITIVE,
        }
    ]

    with patch("crawler.file_processing.create_dart_sql_server_conn") as mock_conn:
        with patch("crawler.file_processing.add_dart_plate_if_doesnt_exist", side_effect=MockedError("Boom!")):
            centre_file.insert_plates_and_wells_from_docs_into_dart(docs_to_insert)

            # logs error and rolls back on exception calling a stored procedure
            assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
            assert centre_file.logging_collection.aggregator_types["TYPE 22"].count_errors == 1
            mock_conn().cursor().rollback.assert_called_once()
            mock_conn().cursor().commit.assert_not_called()
            mock_conn().close.assert_called_once()


def test_insert_plates_and_wells_from_docs_into_dart_non_pending_plate_does_not_update_wells(
    config,
):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)
    docs_to_insert: List[ModifiedRow] = [
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000004",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
            FIELD_RNA_ID: "TC-rna-00000029_H11",
            FIELD_PLATE_BARCODE: "TC-rna-00000029",
            FIELD_LAB_ID: "AP",
            FIELD_COORDINATE: "H11",
            FIELD_RESULT: RESULT_VALUE_POSITIVE,
        }
    ]

    with patch("crawler.file_processing.create_dart_sql_server_conn") as mock_conn:
        with patch("crawler.file_processing.add_dart_plate_if_doesnt_exist", return_value="not pending"):
            centre_file.insert_plates_and_wells_from_docs_into_dart(docs_to_insert)

            # does not call any stored procedure
            assert centre_file.logging_collection.aggregator_types["TYPE 22"].count_errors == 0
            mock_conn().cursor().execute.assert_not_called()
            mock_conn().close.assert_called_once()


def test_insert_plates_and_wells_from_docs_into_dart_none_well_index(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)
    docs_to_insert: List[SampleDoc] = [
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000004",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
            FIELD_RNA_ID: "TC-rna-00000029_H11",
            FIELD_PLATE_BARCODE: "TC-rna-00000029",
            FIELD_LAB_ID: "AP",
            FIELD_COORDINATE: "H11",
            FIELD_RESULT: RESULT_VALUE_POSITIVE,
        }
    ]

    with patch("crawler.file_processing.create_dart_sql_server_conn") as mock_conn:
        with patch(
            "crawler.file_processing.add_dart_plate_if_doesnt_exist",
            return_value=DART_STATE_PENDING,
        ):
            with patch("crawler.db.dart.get_dart_well_index", return_value=None):
                centre_file.insert_plates_and_wells_from_docs_into_dart(docs_to_insert)

                # logs error and rolls back on exception determining well index
                assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
                assert centre_file.logging_collection.aggregator_types["TYPE 22"].count_errors == 1
                mock_conn().cursor().rollback.assert_called_once()
                mock_conn().cursor().commit.assert_not_called()
                mock_conn().close.assert_called_once()


def test_insert_plates_and_wells_from_docs_into_dart_multiple_new_plates(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)
    docs_to_insert = [
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000004",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
            FIELD_RNA_ID: "TC-rna-00000029_A01",
            FIELD_PLATE_BARCODE: "TC-rna-00000029",
            FIELD_COORDINATE: "A01",
            FIELD_LAB_ID: "AP",
            FIELD_RESULT: generate_new_object_for_string(RESULT_VALUE_POSITIVE),
        },
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000006",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABD",
            FIELD_RNA_ID: "TC-rna-00000024_B01",
            FIELD_PLATE_BARCODE: "TC-rna-00000024",
            FIELD_COORDINATE: "B01",
            FIELD_LAB_ID: "AP",
            FIELD_RESULT: generate_new_object_for_string(RESULT_VALUE_POSITIVE),
        },
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000008",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABE",
            FIELD_RNA_ID: "TC-rna-00000024_H01",
            FIELD_PLATE_BARCODE: "TC-rna-00000020",
            FIELD_COORDINATE: "H01",
            FIELD_LAB_ID: "AP",
            FIELD_RESULT: "Void",
        },
    ]

    with patch("crawler.file_processing.create_dart_sql_server_conn") as mock_conn:
        with patch("crawler.file_processing.add_dart_plate_if_doesnt_exist") as mock_add_plate:
            mock_add_plate.return_value = DART_STATE_PENDING
            with patch("crawler.db.dart.get_dart_well_index") as mock_get_well_index:
                test_well_index = 15
                mock_get_well_index.return_value = test_well_index
                with patch("crawler.db.dart.map_mongo_doc_to_dart_well_props") as mock_map:
                    test_well_props = {"prop1": "value1", "test prop": "test value"}
                    mock_map.return_value = test_well_props
                    with patch("crawler.db.dart.set_dart_well_properties") as mock_set_well_props:
                        result = centre_file.insert_plates_and_wells_from_docs_into_dart(docs_to_insert)

                        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0

                        # creates all plates
                        assert mock_add_plate.call_count == 3
                        for doc in docs_to_insert:
                            mock_add_plate.assert_any_call(
                                mock_conn().cursor(),
                                doc[FIELD_PLATE_BARCODE],
                                centre_file.centre_config[CENTRE_KEY_BIOMEK_LABWARE_CLASS],
                            )

                        # well helper method call checks
                        assert mock_get_well_index.call_count == 2
                        assert mock_map.call_count == 2
                        assert mock_set_well_props.call_count == 2
                        for doc in docs_to_insert[:2]:
                            mock_get_well_index.assert_any_call(doc[FIELD_COORDINATE])
                            mock_map.assert_any_call(doc)
                            mock_set_well_props.assert_any_call(
                                mock_conn().cursor(),
                                doc[FIELD_PLATE_BARCODE],
                                test_well_props,
                                test_well_index,
                            )

                        # commits changes
                        mock_conn().cursor().rollback.assert_not_called()
                        assert mock_conn().cursor().commit.call_count == 3
                        mock_conn().close.assert_called_once()

                        assert result is True


def test_insert_plates_and_wells_from_docs_into_dart_single_new_plate_multiple_wells(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)
    plate_barcode = "TC-rna-00000029"
    docs_to_insert = [
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000004",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
            FIELD_RNA_ID: f"{plate_barcode}_A01",
            FIELD_PLATE_BARCODE: plate_barcode,
            FIELD_COORDINATE: "A01",
            FIELD_LAB_ID: "AP",
            FIELD_RESULT: generate_new_object_for_string(RESULT_VALUE_POSITIVE),
        },
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000006",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABD",
            FIELD_RNA_ID: f"{plate_barcode}_A02",
            FIELD_PLATE_BARCODE: plate_barcode,
            FIELD_COORDINATE: "A02",
            FIELD_LAB_ID: "AP",
            FIELD_RESULT: generate_new_object_for_string(RESULT_VALUE_POSITIVE),
        },
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000008",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABE",
            FIELD_RNA_ID: f"{plate_barcode}_A03",
            FIELD_PLATE_BARCODE: plate_barcode,
            FIELD_COORDINATE: "A03",
            FIELD_LAB_ID: "AP",
            FIELD_RESULT: "Void",
        },
        {
            "_id": ObjectId("5f562d9931d9959b92544728"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000009",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABF",
            FIELD_RNA_ID: f"{plate_barcode}_A04",
            FIELD_PLATE_BARCODE: plate_barcode,
            FIELD_COORDINATE: "A04",
            FIELD_LAB_ID: "AP",
            FIELD_RESULT: "Void",
            FIELD_MUST_SEQUENCE: True,
        },
    ]

    with patch("crawler.file_processing.create_dart_sql_server_conn") as mock_conn:
        with patch("crawler.file_processing.add_dart_plate_if_doesnt_exist") as mock_add_plate:
            mock_add_plate.return_value = DART_STATE_PENDING
            with patch("crawler.db.dart.get_dart_well_index") as mock_get_well_index:
                test_well_index = 15
                mock_get_well_index.return_value = test_well_index
                with patch("crawler.db.dart.map_mongo_doc_to_dart_well_props") as mock_map:
                    test_well_props = {"prop1": "value1", "test prop": "test value"}
                    mock_map.return_value = test_well_props
                    with patch("crawler.db.dart.set_dart_well_properties") as mock_set_well_props:
                        centre_file.insert_plates_and_wells_from_docs_into_dart(docs_to_insert)

                        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0

                        # adds a single plate
                        assert mock_add_plate.call_count == 1
                        mock_add_plate.assert_any_call(
                            mock_conn().cursor(),
                            plate_barcode,
                            centre_file.centre_config[CENTRE_KEY_BIOMEK_LABWARE_CLASS],
                        )

                        # calls for well index and to map as expected
                        assert mock_get_well_index.call_count == 2
                        assert mock_map.call_count == 2
                        for doc in docs_to_insert[:2]:
                            mock_get_well_index.assert_any_call(doc[FIELD_COORDINATE])
                            mock_map.assert_any_call(doc)

                        # calls to set well properties as expected
                        assert mock_set_well_props.call_count == 2
                        mock_set_well_props.assert_any_call(
                            mock_conn().cursor(), plate_barcode, test_well_props, test_well_index
                        )

                        # commits changes
                        mock_conn().cursor().rollback.assert_not_called()
                        assert mock_conn().cursor().commit.call_count == 1
                        mock_conn().close.assert_called_once()


def test_docs_to_insert_updated_with_source_plate_uuids_handles_mongo_collection_error(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)
    with patch("crawler.file_processing.get_mongo_collection", side_effect=ValueError("Boom!")):
        result = centre_file.docs_to_insert_updated_with_source_plate_uuids([])

        assert result == []
        assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
        assert centre_file.logging_collection.aggregator_types["TYPE 26"].count_errors == 1


def test_docs_to_insert_updated_with_source_plate_uuids_adds_new_plates(config, mongo_database):
    _, mongo_database = mongo_database
    docs_to_insert: List[ModifiedRow] = [
        {FIELD_PLATE_BARCODE: "123", FIELD_LAB_ID: "AP"},
        {FIELD_PLATE_BARCODE: "456", FIELD_LAB_ID: "MK"},
        {FIELD_PLATE_BARCODE: "456", FIELD_LAB_ID: "MK"},
        {FIELD_PLATE_BARCODE: "789", FIELD_LAB_ID: "CB"},
    ]
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)
    updated_docs = centre_file.docs_to_insert_updated_with_source_plate_uuids(docs_to_insert)
    assert len(updated_docs) == 4

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    assert source_plates_collection.count_documents({}) == 3

    for doc in updated_docs:
        source_plate = source_plates_collection.find_one({FIELD_BARCODE: doc[FIELD_PLATE_BARCODE]})
        assert source_plate is not None
        assert source_plate[FIELD_LH_SOURCE_PLATE_UUID] is not None
        assert doc[FIELD_LH_SOURCE_PLATE_UUID] == source_plate[FIELD_LH_SOURCE_PLATE_UUID]

    assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0


def test_docs_to_insert_updated_with_source_plate_uuids_uses_existing_plates(config, mongo_database):
    _, mongo_database = mongo_database
    source_plates = [
        {FIELD_BARCODE: "123", FIELD_LAB_ID: "AP", FIELD_LH_SOURCE_PLATE_UUID: str(uuid.uuid4())},
        {FIELD_BARCODE: "456", FIELD_LAB_ID: "MK", FIELD_LH_SOURCE_PLATE_UUID: str(uuid.uuid4())},
        {FIELD_BARCODE: "789", FIELD_LAB_ID: "CB", FIELD_LH_SOURCE_PLATE_UUID: str(uuid.uuid4())},
    ]
    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    source_plates_collection.insert_many(source_plates)
    assert source_plates_collection.count_documents({}) == 3  # sanity check

    docs_to_insert: List[ModifiedRow] = [
        {FIELD_PLATE_BARCODE: "123", FIELD_LAB_ID: "AP"},
        {FIELD_PLATE_BARCODE: "456", FIELD_LAB_ID: "MK"},
        {FIELD_PLATE_BARCODE: "456", FIELD_LAB_ID: "MK"},
        {FIELD_PLATE_BARCODE: "789", FIELD_LAB_ID: "CB"},
    ]
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)
    updated_docs = centre_file.docs_to_insert_updated_with_source_plate_uuids(docs_to_insert)
    assert len(updated_docs) == 4

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    assert source_plates_collection.count_documents({}) == 3

    for doc in updated_docs:
        source_plate = source_plates_collection.find_one({FIELD_BARCODE: doc[FIELD_PLATE_BARCODE]})
        assert source_plate is not None
        assert source_plate[FIELD_LH_SOURCE_PLATE_UUID] is not None
        assert doc[FIELD_LH_SOURCE_PLATE_UUID] == source_plate[FIELD_LH_SOURCE_PLATE_UUID]

    assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 0


@pytest.mark.parametrize(
    "filename, expected_type25_errors_count",
    [
        [UNCONSOLIDATED_SURVEILLANCE_FILENAME, 1],
        [CONSOLIDATED_EAGLE_FILENAME, 0],
        [CONSOLIDATED_SURVEILLANCE_FILENAME, 0],
        ["some file", 1],
    ],
)
def test_docs_to_insert_updated_with_source_plate_handles_duplicate_new_barcodes_from_diff_lab_when_not_consolidated(
    config, mongo_database, filename, expected_type25_errors_count
):
    # set up input sample docs to have duplicate plate barcodes from different labs
    _, mongo_database = mongo_database
    docs: List[ModifiedRow] = [
        {FIELD_PLATE_BARCODE: "123", FIELD_LAB_ID: "AP", FIELD_RNA_ID: "123_A01"},
        {FIELD_PLATE_BARCODE: "123", FIELD_LAB_ID: "MK", FIELD_RNA_ID: "123_A02"},  # we expect this one to be rejected
        {FIELD_PLATE_BARCODE: "456", FIELD_LAB_ID: "MK", FIELD_RNA_ID: "456_A03"},
        {FIELD_PLATE_BARCODE: "789", FIELD_LAB_ID: "CB", FIELD_RNA_ID: "789_A04"},
    ]
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile(filename, centre)
    updated_docs = centre_file.docs_to_insert_updated_with_source_plate_uuids(docs)
    assert len(updated_docs) == 4 - expected_type25_errors_count
    assert sum(doc[FIELD_PLATE_BARCODE] == "123" and doc[FIELD_LAB_ID] == "AP" for doc in updated_docs) == 1
    assert sum(doc[FIELD_PLATE_BARCODE] == "456" and doc[FIELD_LAB_ID] == "MK" for doc in updated_docs) == 1
    assert sum(doc[FIELD_PLATE_BARCODE] == "789" and doc[FIELD_LAB_ID] == "CB" for doc in updated_docs) == 1

    if expected_type25_errors_count == 0:
        assert sum(doc[FIELD_PLATE_BARCODE] == "123" and doc[FIELD_LAB_ID] == "MK" for doc in updated_docs) == 1

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    assert source_plates_collection.count_documents({}) == 3

    for doc in updated_docs:
        source_plate = source_plates_collection.find_one({FIELD_BARCODE: doc[FIELD_PLATE_BARCODE]})
        assert source_plate is not None
        assert source_plate[FIELD_LH_SOURCE_PLATE_UUID] is not None
        assert doc[FIELD_LH_SOURCE_PLATE_UUID] == source_plate[FIELD_LH_SOURCE_PLATE_UUID]

    assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == expected_type25_errors_count
    assert centre_file.logging_collection.aggregator_types["TYPE 25"].count_errors == expected_type25_errors_count


def test_docs_to_insert_updated_with_source_plate_not_log_error_when_different_lab_and_consolidated(
    config, mongo_database
):
    # set up input sample docs to have duplicate plate barcodes from different labs
    _, mongo_database = mongo_database
    docs: List[ModifiedRow] = [
        {FIELD_PLATE_BARCODE: "123", FIELD_LAB_ID: "AP"},
        {FIELD_PLATE_BARCODE: "123", FIELD_LAB_ID: "MK"},  # we expect this one to be rejected
        {FIELD_PLATE_BARCODE: "456", FIELD_LAB_ID: "MK"},
        {FIELD_PLATE_BARCODE: "789", FIELD_LAB_ID: "CB"},
    ]
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)
    updated_docs = centre_file.docs_to_insert_updated_with_source_plate_uuids(docs)
    assert len(updated_docs) == 3
    assert sum(doc[FIELD_PLATE_BARCODE] == "123" and doc[FIELD_LAB_ID] == "AP" for doc in docs) == 1
    assert sum(doc[FIELD_PLATE_BARCODE] == "456" and doc[FIELD_LAB_ID] == "MK" for doc in docs) == 1
    assert sum(doc[FIELD_PLATE_BARCODE] == "789" and doc[FIELD_LAB_ID] == "CB" for doc in docs) == 1

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    assert source_plates_collection.count_documents({}) == 3

    for doc in updated_docs:
        source_plate = source_plates_collection.find_one({FIELD_BARCODE: doc[FIELD_PLATE_BARCODE]})
        assert source_plate is not None
        assert source_plate[FIELD_LH_SOURCE_PLATE_UUID] is not None
        assert doc[FIELD_LH_SOURCE_PLATE_UUID] == source_plate[FIELD_LH_SOURCE_PLATE_UUID]

    assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
    assert centre_file.logging_collection.aggregator_types["TYPE 25"].count_errors == 1


def test_docs_to_insert_updated_with_source_plate_handles_duplicate_existing_barcodes_from_diff_lab(
    config, mongo_database
):
    # set up input sample docs to have a plate from a different lab to one already in mongo
    _, mongo_database = mongo_database
    source_plates = [
        {FIELD_BARCODE: "123", FIELD_LAB_ID: "MK", FIELD_LH_SOURCE_PLATE_UUID: str(uuid.uuid4())},
    ]
    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    source_plates_collection.insert_many(source_plates)
    assert source_plates_collection.count_documents({}) == 1  # sanity check

    docs: List[ModifiedRow] = [
        {FIELD_PLATE_BARCODE: "123", FIELD_LAB_ID: "AP"},  # we expect this one to be rejected
        {FIELD_PLATE_BARCODE: "456", FIELD_LAB_ID: "MK"},
        {FIELD_PLATE_BARCODE: "789", FIELD_LAB_ID: "CB"},
    ]
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)
    updated_docs = centre_file.docs_to_insert_updated_with_source_plate_uuids(docs)
    assert len(updated_docs) == 2
    assert sum(doc[FIELD_PLATE_BARCODE] == "456" and doc[FIELD_LAB_ID] == "MK" for doc in docs) == 1
    assert sum(doc[FIELD_PLATE_BARCODE] == "789" and doc[FIELD_LAB_ID] == "CB" for doc in docs) == 1

    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)
    assert source_plates_collection.count_documents({}) == 3

    for doc in updated_docs:
        source_plate = source_plates_collection.find_one({FIELD_BARCODE: doc[FIELD_PLATE_BARCODE]})
        assert source_plate is not None
        assert source_plate[FIELD_LH_SOURCE_PLATE_UUID] is not None
        assert doc[FIELD_LH_SOURCE_PLATE_UUID] == source_plate[FIELD_LH_SOURCE_PLATE_UUID]

    assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
    assert centre_file.logging_collection.aggregator_types["TYPE 25"].count_errors == 1


def test_docs_to_insert_updated_with_cog_uk_ids_adds_cog_uk_ids(config, baracoda):
    original_docs: List[ModifiedRow] = [
        {"_id": ObjectId("5f562d9931d9959b92544728")},
        {"_id": ObjectId("5f562d9931d9959b92544729")},
        {"_id": ObjectId("5f562d9931d9959b9254472a")},
    ]

    centre = Centre(config, config.CENTRES[0])
    prefix = centre.centre_config[CENTRE_KEY_PREFIX]
    centre_file = CentreFile("some file", centre)
    actual = centre_file.docs_to_insert_updated_with_cog_uk_ids(original_docs)

    assert actual[0][FIELD_MONGO_COG_UK_ID] == f"{prefix}-123ABC"
    assert actual[1][FIELD_MONGO_COG_UK_ID] == f"{prefix}-123ABD"
    assert actual[2][FIELD_MONGO_COG_UK_ID] == f"{prefix}-123ABE"


def test_docs_to_insert_updated_with_cog_uk_ids_logs_baracoda_error_and_returns_empty_list(config):
    original_docs: List[ModifiedRow] = [
        {"_id": ObjectId("5f562d9931d9959b92544728")},
        {"_id": ObjectId("5f562d9931d9959b92544729")},
        {"_id": ObjectId("5f562d9931d9959b9254472a")},
    ]

    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    with patch("crawler.file_processing.logger") as logger:
        actual = centre_file.docs_to_insert_updated_with_cog_uk_ids(original_docs)

    assert actual == []
    assert centre_file.logging_collection.get_count_of_all_errors_and_criticals() == 1
    assert centre_file.logging_collection.aggregator_types["TYPE 35"].count_errors == 1
    logger.critical.assert_called_once()
    logger.exception.assert_called_once()


# Test is_current set to true for latest results only
def test_is_current_correctly_set(config, mlwh_connection):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some_file.csv", centre)

    docs: List[SampleDoc] = [
        {
            "_id": ObjectId("5f562d9931d9959b92544721"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000004",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABC",
            FIELD_RNA_ID: "TC-rna-00000029_A01",
            FIELD_PLATE_BARCODE: "TC-rna-00000029",
            FIELD_COORDINATE: "H11",
            FIELD_RESULT: "Negative",
            FIELD_SOURCE: "Test Centre",
            FIELD_MONGO_LAB_ID: "TC",
        },
        {
            "_id": ObjectId("5f562d9931d9959b92544722"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000004",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABD",
            FIELD_RNA_ID: "TC-rna-00000029_A01",
            FIELD_PLATE_BARCODE: "TC-rna-00000029",
            FIELD_COORDINATE: "H11",
            FIELD_RESULT: "Positive",
            FIELD_SOURCE: "Test Centre",
            FIELD_MONGO_LAB_ID: "TC",
        },
        {
            "_id": ObjectId("5f562d9931d9959b92544723"),
            FIELD_ROOT_SAMPLE_ID: "ABC00000005",
            FIELD_MONGO_COG_UK_ID: "TEST-123ABE",
            FIELD_RNA_ID: "TC-rna-00000029_B01",
            FIELD_PLATE_BARCODE: "TC-rna-00000029",
            FIELD_COORDINATE: "H11",
            FIELD_RESULT: "Negative",
            FIELD_SOURCE: "Test Centre",
            FIELD_MONGO_LAB_ID: "TC",
        },
    ]

    centre_file.insert_samples_from_docs_into_mlwh(docs)

    cursor = mlwh_connection.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME}")
    rows = cursor.fetchall()
    cursor.close()

    assert len(rows) == 3
    assert rows[0][MLWH_IS_CURRENT] == 0
    assert rows[1][MLWH_IS_CURRENT] == 1
    assert rows[2][MLWH_IS_CURRENT] == 1


def test_center_can_download_only_all_files(config, tmpdir, downloadable_files):
    centre = Centre(config, config.CENTRES[0])

    centre.centre_config["sftp_root_read"] = "/sftp"

    with patch("crawler.file_processing.Centre.get_download_dir", return_value=tmpdir.realpath()):
        centre.download_csv_files()

        file_list = sorted([os.path.basename(file_path) for file_path in tmpdir.listdir()])
        assert file_list == [
            "AP_sanger_report_200423_2214.csv",
            "AP_sanger_report_200423_2215.csv",
            "AP_sanger_report_200423_2216.csv",
            "AP_sanger_report_200423_2217.csv",
            "AP_sanger_report_200423_2218.csv",
        ]


def test_center_not_download_if_nothing_recent(config, tmpdir, downloadable_files):
    very_old_time = datetime.now().replace(year=1979)
    tuple_time = (very_old_time.timestamp(), very_old_time.timestamp())

    with get_sftp_connection(config) as sftp:
        for filename in downloadable_files:
            sftp.sftp_client.utime(filename, tuple_time)

    centre = Centre(config, config.CENTRES[0])

    centre.centre_config["sftp_root_read"] = "/sftp"

    with patch("crawler.file_processing.Centre.get_download_dir", return_value=tmpdir.realpath()):
        centre.download_csv_files()

        file_list = sorted([os.path.basename(file_path) for file_path in tmpdir.listdir()])
        assert file_list == []


def test_center_can_download_only_recent_files(config, tmpdir, downloadable_files):
    very_old_time = datetime.now().replace(year=1979)
    tuple_time = (very_old_time.timestamp(), very_old_time.timestamp())

    with get_sftp_connection(config) as sftp:
        sftp.sftp_client.utime(downloadable_files[1], tuple_time)
        sftp.sftp_client.utime(downloadable_files[3], tuple_time)

    centre = Centre(config, config.CENTRES[0])

    centre.centre_config["sftp_root_read"] = "/sftp"

    with patch("crawler.file_processing.Centre.get_download_dir", return_value=tmpdir.realpath()):
        centre.download_csv_files()

        file_list = sorted([os.path.basename(file_path) for file_path in tmpdir.listdir()])
        assert file_list == [
            "AP_sanger_report_200423_2214.csv",
            "AP_sanger_report_200423_2216.csv",
            "AP_sanger_report_200423_2218.csv",
        ]


@pytest.mark.parametrize(
    "filename, mode, expected_value",
    [
        ["AP_sanger_report_200423_2218.csv", 33188, True],
        ["AP-123456.txt", 33188, False],
        ["/project-heron_cambridge-az", 16877, False],
    ],
)
def test_can_identify_is_csv_file(config, filename, mode, expected_value):
    centre = Centre(config, config.CENTRES[0])
    assert centre.is_csv_file(mode, filename) is expected_value
