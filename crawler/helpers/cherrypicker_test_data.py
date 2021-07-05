import csv
from datetime import datetime
import logging
import os
import random
import requests


logger = logging.getLogger(__name__)


def make_coords_list():
    for column in "ABCDEFGH":
        for row in range(1, 13):
            yield "%s%02d" % (column, row)


WELL_COORDS = list(make_coords_list())
PLATE_SIZE = len(WELL_COORDS)

SAMPLES_FILE_HEADERS = [
    "Root Sample ID",
    "Viral Prep ID",
    "RNA ID",
    "RNA-PCR ID",
    "Result",
    "Date Tested",
    "Lab ID",
    "CH1-Target",
    "CH1-Result",
    "CH1-Cq",
    "CH2-Target",
    "CH2-Result",
    "CH2-Cq",
    "CH3-Target",
    "CH3-Result",
    "CH3-Cq",
    "CH4-Target",
    "CH4-Result",
    "CH4-Cq",
]

PRINT_FILE_HEADERS = ["barcode", "text"]
BARACODA_PREFIX = "TEST"


def flatten(nested_list: list) -> list:
    return [item for sublist in nested_list for item in sublist]


def generate_baracoda_barcodes(num_required: int) -> list:
    baracoda_url = f"http://uat.baracoda.psd.sanger.ac.uk/barcodes_group/{BARACODA_PREFIX}/new?count={num_required}"
    response = requests.post(baracoda_url, data={})
    response_json = response.json()
    barcodes: list = response_json["barcodes_group"]["barcodes"]
    return barcodes


def create_barcodes(num_required: int) -> list:
    # call Baracoda here and fetch a set of barcodes with the prefix we want
    logger.info(f"Num barcodes required from Baracoda = {num_required}")
    list_barcodes = generate_baracoda_barcodes(num_required)
    return list_barcodes


def create_root_sample_id(barcode: str, well_num: int) -> str:
    return "RSID-%s%s" % (barcode, str(well_num).zfill(4))


def create_viral_prep_id(barcode: str, well_num: int, well_coordinate: str) -> str:
    return "VPID-%s%s_%s" % (barcode, str(well_num).zfill(4), well_coordinate)


def create_rna_id(barcode: str, well_coordinate: str) -> str:
    return "%s_%s" % (barcode, well_coordinate)


def create_rna_pcr_id(barcode: str, well_num: int, well_coordinate: str) -> str:
    return "RNA_PCR-%s%s_%s" % (barcode, str(well_num).zfill(4), well_coordinate)


def create_test_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def create_row(dt: datetime, well_index: int, result: str, barcode: str) -> list:
    well_coordinate = WELL_COORDS[well_index]
    well_num = well_index + 1
    return [
        create_root_sample_id(barcode, well_num),
        create_viral_prep_id(barcode, well_num, well_coordinate),
        create_rna_id(barcode, well_coordinate),
        create_rna_pcr_id(barcode, well_num, well_coordinate),
        result,
        create_test_timestamp(dt),
        "AP",
    ] + [""] * 12


def create_plate_rows(dt, num_positives, plate_barcode):
    num_negatives = PLATE_SIZE - num_positives
    outcomes = ["Positive"] * num_positives + ["Negative"] * num_negatives
    random.shuffle(outcomes)

    return [create_row(dt, i, outcome, plate_barcode) for i, outcome in enumerate(outcomes)]


def flat_list_of_positives_per_plate(plate_specs: list) -> list:
    # Turn [[2, 5], [3, 10]] into [5, 5, 10, 10, 10]
    return flatten([[specs[1]] * specs[0] for specs in plate_specs])


def create_csv_rows(plate_specs: list, dt: datetime, list_barcodes: list) -> list:
    pos_per_plate = flat_list_of_positives_per_plate(plate_specs)

    # Create lists of csv rows for each plate
    # i.e. [ [ [ "plate1", "row1" ], [ "plate1", "row2" ] ], [ [ "plate2", "row1" ] ] ]
    plate_rows = [create_plate_rows(dt, pos_per_plate[i], list_barcodes[i]) for i in range(len(pos_per_plate))]

    # Flatten before returning
    # i.e. [ [ "plate1", "row1" ], [ "plate1", "row2" ], [ "plate2", "row1" ] ]
    return flatten(plate_rows)


def write_plates_file(rows: list, path: str, filename: str) -> None:
    logger.info(f"Writing to file: {filename}")

    try:
        if not os.path.isdir(path):
            os.makedirs(path)

        full_path = os.path.join(path, filename)
        with open(full_path, mode="w") as plates_file:
            plates_writer = csv.writer(plates_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # write header row
            plates_writer.writerow(SAMPLES_FILE_HEADERS)

            # write well rows
            for row in rows:
                plates_writer.writerow(row)

    except Exception as e:
        logger.error(f"Exception: {e}")
        raise
    else:
        logger.info(f"Test data plates file written: {filename}")


def create_barcode_meta(plate_specs: list, list_barcodes: list) -> list:
    logger.info("Creating metadata for barcodes")

    pos_per_plate = flat_list_of_positives_per_plate(plate_specs)
    return [[list_barcodes[i], f"number of positives: {pos_per_plate[i]}"] for i in range(len(pos_per_plate))]
