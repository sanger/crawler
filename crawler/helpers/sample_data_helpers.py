import re


def normalise_plate_coordinate(coordinate):
    """Takes a plate coordinate in the format A1 through H12 and pads single digit columns with a zero if not already
    padded.  i.e. A1 becomes A01.  A12 would stay as A12.  Unless the input is a letter followed by a single digit,
    the return value will be unmodified.
    """
    match = re.match(r"^([A-H])([1-9])$", coordinate)

    if match is None:
        return coordinate

    return f"{match[1]}0{match[2]}"
