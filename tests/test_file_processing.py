import logging
import logging.config
import shutil
import os
from io import StringIO
from crawler.helpers import current_time
from unittest.mock import patch
from csv import DictReader
import pytest

from crawler.file_processing import (
    Centre,
    CentreFile,
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

def test_extract_fields(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile('some file', centre)

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
    assert centre_file.extract_fields({"RNA ID": "ABC123_H0"}, barcode_field, barcode_regex) == ("", "")
    assert centre_file.extract_fields({"RNA ID": "ABC123H0"}, barcode_field, barcode_regex) == ("", "")
    assert centre_file.extract_fields({"RNA ID": "AB23_H01"}, barcode_field, barcode_regex) == ("AB23", "H01")


def test_add_extra_fields(config):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile('some file', centre)

    extra_fields_added = [
        {
            "id": "1",
            "RNA ID": "RNA_0043_H09",
            "plate_barcode": "RNA_0043",
            "source": "Alderley",
            "coordinate": "H09",
        }
    ]

    with StringIO() as fake_csv:
        fake_csv.write("id,RNA ID\n")
        fake_csv.write("1,RNA_0043_H09\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)

        augmented_data = centre_file.add_extra_fields(csv_to_test_reader)
        assert augmented_data == extra_fields_added
        assert len(centre_file.errors) == 0

    wrong_barcode = [
        {
            "id": "1",
            "RNA ID": "RNA_0043_",
            "plate_barcode": "",
            "source": "Alderley",
            "coordinate": "",
        }
    ]

    with StringIO() as fake_csv:
        fake_csv.write("id,RNA ID\n")
        fake_csv.write("1,RNA_0043_\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)

        augmented_data = centre_file.add_extra_fields(csv_to_test_reader)
        assert augmented_data == wrong_barcode
        assert len(centre_file.errors) == 1


def test_get_download_dir(config):
    for centre_config in config.CENTRES:
        centre = Centre(config, centre_config)

        assert (
            centre.get_download_dir() == f"{config.DIR_DOWNLOADED_DATA}{centre['prefix']}/"
        )

def test_check_for_required_fields(config):
    config.CENTRES[0]["barcode_field"]="RNA ID"
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile('some file', centre)

    with pytest.raises(CentreFileError, match=r"Cannot read CSV fieldnames"):
        with StringIO() as fake_csv:

            csv_to_test_reader = DictReader(fake_csv)
            assert centre_file.check_for_required_fields(csv_to_test_reader) is None

    config.CENTRES[0]["barcode_field"]="RNA ID"
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile('some file', centre)

    with pytest.raises(CentreFileError, match=r".* missing in CSV file"):
        with StringIO() as fake_csv:
            fake_csv.write("id,RNA ID\n")
            fake_csv.write("1,RNA_0043_\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            assert (
                centre_file.check_for_required_fields(csv_to_test_reader) is None
            )

    config.CENTRES[0]["barcode_field"]="RNA ID"
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile('some file', centre)

    with StringIO() as fake_csv:
        fake_csv.write(
            f"{FIELD_ROOT_SAMPLE_ID},{FIELD_RNA_ID},{FIELD_RESULT},{FIELD_DATE_TESTED},"
            f"{FIELD_LAB_ID}\n"
        )
        fake_csv.write("1,RNA_0043,Positive,today,MK\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)

        assert centre_file.check_for_required_fields(csv_to_test_reader) is None


def test_archival_prepared_sample_conversor_changes_data(config):
    timestamp = '20/12/20'

    centre = Centre(config, config.CENTRES[0])

    with patch(
        "crawler.file_processing.current_time", return_value=timestamp,
    ):
        centre_file = CentreFile('some file', centre)

        val = centre_file.archival_prepared_sample_conversor({'_id': '1234', 'name': '4567'}, timestamp)
        assert val == {'sample_object_id': '1234', 'archived_at': '20/12/20', 'name': '4567'}


def test_archival_prepared_samples_adds_timestamp(config):
    timestamp = '20/12/20'

    centre = Centre(config, config.CENTRES[0])

    with patch(
        "crawler.file_processing.current_time", return_value=timestamp,
    ):
        centre_file = CentreFile('some file', centre)

        val = centre_file.archival_prepared_samples([{'_id': '1234', 'name': '4567'}, {'_id': '4567', 'name': '1234'}])
        assert val == [
            {'sample_object_id': '1234', 'archived_at': '20/12/20', 'name': '4567'},
            {'sample_object_id': '4567', 'archived_at': '20/12/20', 'name': '1234'}
        ]


def test_archive_old_samples(config, testing_samples, samples_history_collection_accessor, samples_collection_accessor):
    sample_object_ids = list(map(lambda x: x['_id'], testing_samples))
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile('some file', centre)
    current_archived_samples = samples_history_collection_accessor.find({"sample_object_id": {"$in": sample_object_ids}})
    assert current_archived_samples.count() == 0
    centre_file.archive_old_samples(testing_samples)

    archived_samples = samples_history_collection_accessor.find({"sample_object_id": {"$in": sample_object_ids}})
    assert len(testing_samples) == archived_samples.count()
    assert samples_collection_accessor.find({"Root Sample ID": {"$in": sample_object_ids}}).count() == 0

def test_archive_old_samples_without_previous_samples(config, samples_history_collection_accessor, samples_collection_accessor):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile('some file', centre)

    assert samples_collection_accessor.count() == 0
    assert samples_history_collection_accessor.count() == 0

    centre_file.archive_old_samples([{"Root Sample ID": "1"}])

    assert samples_history_collection_accessor.count() == 0
    assert samples_collection_accessor.count() == 0


def test_backup_file(config, tmpdir):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile('AP_sanger_report_200503_2338.csv', centre)


def test_get_download_dir(config):
    centre = Centre(config, config.CENTRES[0])

    assert centre.get_download_dir() == 'tests/files/ALDP/'

def test_get_filename_with_checksum():
    return True

def test_set_state_for_file_when_file_in_black_list():
    return False

def test_set_state_for_file_when_never_seen_before():
    return False

def test_set_state_for_file_when_in_error_folder():
    return False

def test_set_state_for_file_when_in_success_folder():
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

