import logging
import logging.config
import shutil
import os
from io import StringIO
from crawler.helpers import current_time
from unittest.mock import patch
from csv import DictReader
import pytest
import datetime

from tempfile import mkstemp

from crawler.file_processing import (
    Centre,
    CentreFile,
    CentreFileState,
    SUCCESSES_DIR,
    ERRORS_DIR,
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
)
from crawler.exceptions import CentreFileError
from crawler.db import get_mongo_collection


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
            ["adfsadf", "0b3f0de9aa86ae013f5b013a4bb189ba"],
            "200601_1414",
        )

        try:
            centre = Centre(config, config.CENTRES[0])
            centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)

            assert centre_file.checksum_match("successes") == True
        finally:
            for tmpfile_for_list in list_files:
                os.remove(tmpfile_for_list)


def test_row_invalid_structure(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    # Not maching regexp
    assert not centre_file.row_valid_structure(
        {"Root Sample ID": "asdf", "Result": "Positive", "RNA ID": "", "Date tested": "adsf"}, 6
    ), "No RNA id"

    assert not centre_file.row_valid_structure(
        {"Root Sample ID": "asdf", "Result": "", "RNA ID": "", "Date Tested": "date"}, 1
    ), "Not barcode"

    # All required but all empty
    assert not centre_file.row_valid_structure(
        {"Root Sample ID": "", "Result": "", "RNA ID": "", "Date tested": ""}, 4
    ), "All are empty"


def test_row_valid_structure(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    assert centre_file.row_valid_structure(
        {"Root Sample ID": "asdf", "Result": "asdf", "RNA ID": "ASDF_A01", "Date tested": "asdf"}, 5
    )

    assert centre_file.row_valid_structure(
        {"Root Sample ID": "asdf", "Result": "", "RNA ID": "ASDF_A01", "Date tested": ""}, 5
    )


def test_extract_fields(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    barcode_field = "RNA ID"
    barcode_regex = r"^(.*)_([A-Z]\d\d)$"
    assert centre_file.extract_fields({"RNA ID": "ABC123_H01"}, barcode_field, barcode_regex) == (
        "ABC123",
        "H01",
    )
    assert centre_file.extract_fields({"RNA ID": "ABC123_A00"}, barcode_field, barcode_regex) == (
        "ABC123",
        "A00",
    )
    assert centre_file.extract_fields({"RNA ID": "ABC123_H0"}, barcode_field, barcode_regex) == (
        "",
        "",
    )
    assert centre_file.extract_fields({"RNA ID": "ABC123H0"}, barcode_field, barcode_regex) == (
        "",
        "",
    )
    assert centre_file.extract_fields({"RNA ID": "AB23_H01"}, barcode_field, barcode_regex) == (
        "AB23",
        "H01",
    )


def test_format_and_filter_rows(config):
    timestamp = "some timestamp"
    with patch("crawler.file_processing.current_time", return_value=timestamp):
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("some file", centre)

        extra_fields_added = [
            {
                "Root Sample ID": "1",
                "RNA ID": "RNA_0043_H09",
                "plate_barcode": "RNA_0043",
                "source": "Alderley",
                "coordinate": "H09",
                "line_number": 1,
                "file_name": "some file",
                "file_name_date": None,
                "_created": timestamp,
                "_updated": timestamp,
            }
        ]

        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID\n")
            fake_csv.write("1,RNA_0043_H09\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.format_and_filter_rows(csv_to_test_reader)
            assert augmented_data == extra_fields_added
            assert len(centre_file.errors) == 0

        wrong_barcode = [
            {
                "Root Sample ID": "1",
                "RNA ID": "RNA_0043_",
                "plate_barcode": "",
                "source": "Alderley",
                "coordinate": "",
            }
        ]

        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID\n")
            fake_csv.write("1,RNA_0043_\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.format_and_filter_rows(csv_to_test_reader)
            assert augmented_data == []
            assert len(centre_file.errors) == 1


def test_format_and_filter_rows_parsing_filename(config):
    timestamp = "some timestamp"
    with patch("crawler.file_processing.current_time", return_value=timestamp):
        centre = Centre(config, config.CENTRES[0])
        centre_file = CentreFile("ASDF_200507_1340.csv", centre)

        extra_fields_added = [
            {
                "Root Sample ID": "1",
                "RNA ID": "RNA_0043_H09",
                "plate_barcode": "RNA_0043",
                "source": "Alderley",
                "coordinate": "H09",
                "line_number": 1,
                "file_name": "ASDF_200507_1340.csv",
                "file_name_date": datetime.datetime(2020, 5, 7, 13, 40),
                "_created": timestamp,
                "_updated": timestamp,
            },
            {
                "Root Sample ID": "2",
                "RNA ID": "RNA_0043_B08",
                "plate_barcode": "RNA_0043",
                "source": "Alderley",
                "coordinate": "B08",
                "line_number": 2,
                "file_name": "ASDF_200507_1340.csv",
                "file_name_date": datetime.datetime(2020, 5, 7, 13, 40),
                "_created": timestamp,
                "_updated": timestamp,
            },
        ]

        with StringIO() as fake_csv:
            fake_csv.write("Root Sample ID,RNA ID\n")
            fake_csv.write("1,RNA_0043_H09\n")
            fake_csv.write("2,RNA_0043_B08\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            augmented_data = centre_file.format_and_filter_rows(csv_to_test_reader)
            print(augmented_data)
            assert augmented_data == extra_fields_added
            assert len(centre_file.errors) == 0


def test_get_download_dir(config):
    for centre_config in config.CENTRES:
        centre = Centre(config, centre_config)

        assert centre.get_download_dir() == f"{config.DIR_DOWNLOADED_DATA}{centre['prefix']}/"


def test_check_for_required_headers(config):
    config.CENTRES[0]["barcode_field"] = "RNA ID"
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    with pytest.raises(CentreFileError, match=r"Cannot read CSV fieldnames"):
        with StringIO() as fake_csv:

            csv_to_test_reader = DictReader(fake_csv)
            assert centre_file.check_for_required_headers(csv_to_test_reader) is None

    config.CENTRES[0]["barcode_field"] = "RNA ID"
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    with pytest.raises(CentreFileError, match=r".* missing in CSV file"):
        with StringIO() as fake_csv:
            fake_csv.write("id,RNA ID\n")
            fake_csv.write("1,RNA_0043_\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            assert centre_file.check_for_required_headers(csv_to_test_reader) is None

    config.CENTRES[0]["barcode_field"] = "RNA ID"
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    with StringIO() as fake_csv:
        fake_csv.write(
            f"{FIELD_ROOT_SAMPLE_ID},{FIELD_RNA_ID},{FIELD_RESULT},{FIELD_DATE_TESTED},"
            f"{FIELD_LAB_ID}\n"
        )
        fake_csv.write("1,RNA_0043,Positive,today,MK\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)

        assert centre_file.check_for_required_headers(csv_to_test_reader) is None


def test_archival_prepared_sample_conversor_changes_data(config):
    timestamp = "20/12/20"

    centre = Centre(config, config.CENTRES[0])

    with patch(
        "crawler.file_processing.current_time", return_value=timestamp,
    ):
        centre_file = CentreFile("some file", centre)

        val = centre_file.archival_prepared_sample_conversor(
            {"_id": "1234", "name": "4567"}, timestamp
        )
        assert val == {"sample_object_id": "1234", "archived_at": "20/12/20", "name": "4567"}


def test_archival_prepared_samples_adds_timestamp(config):
    timestamp = "20/12/20"

    centre = Centre(config, config.CENTRES[0])

    with patch(
        "crawler.file_processing.current_time", return_value=timestamp,
    ):
        centre_file = CentreFile("some file", centre)

        val = centre_file.archival_prepared_samples(
            [{"_id": "1234", "name": "4567"}, {"_id": "4567", "name": "1234"}]
        )
        assert val == [
            {"sample_object_id": "1234", "archived_at": "20/12/20", "name": "4567"},
            {"sample_object_id": "4567", "archived_at": "20/12/20", "name": "1234"},
        ]


def test_archive_old_samples(
    config, testing_samples, samples_history_collection_accessor, samples_collection_accessor
):
    sample_object_ids = list(map(lambda x: x["_id"], testing_samples))
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)
    current_archived_samples = samples_history_collection_accessor.find(
        {"sample_object_id": {"$in": sample_object_ids}}
    )
    assert current_archived_samples.count() == 0
    centre_file.archive_old_samples(testing_samples)

    archived_samples = samples_history_collection_accessor.find(
        {"sample_object_id": {"$in": sample_object_ids}}
    )
    assert len(testing_samples) == archived_samples.count()
    assert (
        samples_collection_accessor.find({"Root Sample ID": {"$in": sample_object_ids}}).count()
        == 0
    )


def test_archive_old_samples_without_previous_samples(
    config, samples_history_collection_accessor, samples_collection_accessor
):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("some file", centre)

    assert samples_collection_accessor.count() == 0
    assert samples_history_collection_accessor.count() == 0

    centre_file.archive_old_samples([{"Root Sample ID": "1"}])

    assert samples_history_collection_accessor.count() == 0
    assert samples_collection_accessor.count() == 0


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
        centre_file.errors.append("Some error happened")
        centre_file.backup_file()

        assert len(errors_folder.listdir()) == 1
        assert len(success_folder.listdir()) == 0

        filename_with_timestamp = os.path.basename(errors_folder.listdir()[0])
        assert filename in filename_with_timestamp


def test_file_name_date_parses_right(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)
    assert centre_file.file_name_date().year == 2020
    assert centre_file.file_name_date().month == 5
    assert centre_file.file_name_date().day == 3
    assert centre_file.file_name_date().hour == 23
    assert centre_file.file_name_date().minute == 38


def test_get_download_dir(config):
    centre = Centre(config, config.CENTRES[0])

    assert centre.get_download_dir() == "tests/files/ALDP/"


def test_set_state_for_file_when_file_in_black_list(config, blacklist_for_centre):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)
    centre_file.set_state_for_file()

    assert centre_file.file_state == CentreFileState.FILE_IN_BLACKLIST


def test_set_state_for_file_when_never_seen_before(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("AP_sanger_report_200503_2338.csv", centre)
    centre_file.set_state_for_file()

    assert centre_file.file_state == CentreFileState.FILE_NOT_PROCESSED_YET


def test_set_state_for_file_when_in_error_folder(config, tmpdir):
    with patch.dict(config.CENTRES[0], {"backups_folder": tmpdir.realpath()}):
        errors_folder = tmpdir.mkdir(ERRORS_DIR)
        success_folder = tmpdir.mkdir(SUCCESSES_DIR)

        # configure to use the backups folder for this test

        centre = Centre(config, config.CENTRES[0])

        # create a backup of the file inside the errors directory as if previously processed there
        filename = "AP_sanger_report_200518_2132.csv"
        centre_file = CentreFile(filename, centre)
        centre_file.errors.append("Some error happened")
        centre_file.backup_file()

        assert len(errors_folder.listdir()) == 1

        # check the file state again now the error version exists
        centre_file.set_state_for_file()

        assert centre_file.file_state == CentreFileState.FILE_PROCESSED_WITH_ERROR


def test_set_state_for_file_when_in_success_folder(config):
    return False


def test_process_files(mongo_database, config, testing_files_for_process):
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

