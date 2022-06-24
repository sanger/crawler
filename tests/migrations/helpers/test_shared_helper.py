from migrations.helpers.shared_helper import (extract_barcodes,
                                              valid_datetime_string)

# ----- valid_datetime_string tests -----


def test_valid_datetime_string():
    result1 = valid_datetime_string("")
    assert result1 is False
    result2 = valid_datetime_string("201209_0000")
    assert result2 is True


def test_extract_barcodes_read_barcodes(config):
    filepath = "./tests/data/populate_old_plates.csv"

    assert extract_barcodes(config, filepath) == ["123"]


    filepath = "./tests/data/populate_old_plates_2.csv"

    assert extract_barcodes(config, filepath) == ["123", "456"]
