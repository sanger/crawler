from csv import DictReader
from io import StringIO

import pytest

from crawler.constants import (
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
)
from crawler.exceptions import CentreFileError
from crawler.helpers import (
    add_extra_fields,
    check_for_required_fields,
    extract_fields,
    get_config,
    get_download_dir,
    merge_daily_files,
)


def test_get_config():
    with pytest.raises(ModuleNotFoundError):
        get_config("x.y.z")


def test_extract_fields():
    barcode_field = "RNA ID"
    barcode_regex = r"^(.*)_([A-Z]\d\d)$"
    assert extract_fields({"RNA ID": "ABC123_H01"}, barcode_field, barcode_regex) == (
        "ABC123",
        "H01",
    )
    assert extract_fields({"RNA ID": "ABC123_A00"}, barcode_field, barcode_regex) == (
        "ABC123",
        "A00",
    )
    assert extract_fields({"RNA ID": "ABC123_H0"}, barcode_field, barcode_regex) == ("", "")
    assert extract_fields({"RNA ID": "ABC123H0"}, barcode_field, barcode_regex) == ("", "")
    assert extract_fields({"RNA ID": "AB23_H01"}, barcode_field, barcode_regex) == ("AB23", "H01")


def test_add_extra_fields(config):
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

        errors, augmented_data = add_extra_fields(csv_to_test_reader, config.CENTRES[0], [])
        assert augmented_data == extra_fields_added
        assert len(errors) == 0

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

        errors, augmented_data = add_extra_fields(csv_to_test_reader, config.CENTRES[0], [])
        assert augmented_data == wrong_barcode
        assert len(errors) == 1


def test_get_download_dir(config):
    for centre in config.CENTRES:
        assert (
            get_download_dir(config, centre) == f"{config.DIR_DOWNLOADED_DATA}{centre['prefix']}/"
        )


def test_check_for_required_fields(config):
    with pytest.raises(CentreFileError, match=r"Cannot read CSV fieldnames"):
        with StringIO() as fake_csv:

            csv_to_test_reader = DictReader(fake_csv)

            assert check_for_required_fields(csv_to_test_reader, {}) is None

    with pytest.raises(CentreFileError, match=r".* missing in CSV file"):
        with StringIO() as fake_csv:
            fake_csv.write("id,RNA ID\n")
            fake_csv.write("1,RNA_0043_\n")
            fake_csv.seek(0)

            csv_to_test_reader = DictReader(fake_csv)

            assert (
                check_for_required_fields(csv_to_test_reader, {"barcode_field": "RNA ID"}) is None
            )

    with StringIO() as fake_csv:
        fake_csv.write(
            f"{FIELD_ROOT_SAMPLE_ID},{FIELD_RNA_ID},{FIELD_RESULT},{FIELD_DATE_TESTED},"
            f"{FIELD_LAB_ID}\n"
        )
        fake_csv.write("1,RNA_0043,Positive,today,MK\n")
        fake_csv.seek(0)

        csv_to_test_reader = DictReader(fake_csv)

        assert check_for_required_fields(csv_to_test_reader, {"barcode_field": "RNA ID"}) is None


def test_merge_daily_files(config):
    # run this first to create the file to test
    master_file_name = "MK_sanger_report_200518_2206_master.csv"
    assert merge_daily_files(config, config.CENTRES[1]) == master_file_name

    master_file = f"{get_download_dir(config, config.CENTRES[1])}{master_file_name}"
    test_file = f"{get_download_dir(config, config.CENTRES[1])}test_merge_daily_files.csv"

    with open(master_file, "r") as mf:
        with open(test_file, "r") as tf:
            assert mf.read() == tf.read()
