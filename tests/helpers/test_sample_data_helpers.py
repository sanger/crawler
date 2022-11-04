import pytest

from crawler.helpers.sample_data_helpers import normalise_plate_coordinate


@pytest.mark.parametrize(
    "input, expected",
    [
        ("", ""),
        ("Hello", "Hello"),
        ("A0", "A0"),  # not a valid column, so no normalisation applied
        ("I1", "I1"),  # not a valid row, so no normalisation applied
        ("A1", "A01"),
        ("A2", "A02"),
        ("A3", "A03"),
        ("A4", "A04"),
        ("A5", "A05"),
        ("A6", "A06"),
        ("A7", "A07"),
        ("A8", "A08"),
        ("A9", "A09"),
        ("A10", "A10"),
        ("A11", "A11"),
        ("A12", "A12"),
        ("B1", "B01"),
        ("C1", "C01"),
        ("D1", "D01"),
        ("E1", "E01"),
        ("F1", "F01"),
        ("G1", "G01"),
        ("H1", "H01"),
    ],
)
def test_normalise_plate_coordinate_does_correct_normalisation(input, expected):
    assert normalise_plate_coordinate(input) == expected
