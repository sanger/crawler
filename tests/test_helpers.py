from csv import DictReader
from io import StringIO

from crawler.helpers import add_extra_fields, extract_fields


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


def test_add_extra_fields(centre_details):
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

        errors, augmented_data = add_extra_fields(csv_to_test_reader, centre_details[0], [])
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

        errors, augmented_data = add_extra_fields(csv_to_test_reader, centre_details[0], [])
        assert augmented_data == wrong_barcode
        assert len(errors) == 1
