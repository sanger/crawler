import os
import csv
import random
import requests


WELL_COORDS = [
    "A01","A02","A03","A04","A05","A06","A07","A08","A09","A10","A11","A12",
    "B01","B02","B03","B04","B05","B06","B07","B08","B09","B10","B11","B12",
    "C01","C02","C03","C04","C05","C06","C07","C08","C09","C10","C11","C12",
    "D01","D02","D03","D04","D05","D06","D07","D08","D09","D10","D11","D12",
    "E01","E02","E03","E04","E05","E06","E07","E08","E09","E10","E11","E12",
    "F01","F02","F03","F04","F05","F06","F07","F08","F09","F10","F11","F12",
    "G01","G02","G03","G04","G05","G06","G07","G08","G09","G10","G11","G12",
    "H01","H02","H03","H04","H05","H06","H07","H08","H09","H10","H11","H12"
]

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
    "CH4-Cq"
]

PRINT_FILE_HEADERS = [
    "barcode",
    "text"
]

BARACODA_PREFIX = 'TEST'

def flatten(t):
    return [item for sublist in t for item in sublist]


def generate_baracoda_barcodes(num_required) -> list:
    baracoda_url = f"http://uat.baracoda.psd.sanger.ac.uk/barcodes_group/{BARACODA_PREFIX}/new?count={num_required}"
    response = requests.post(baracoda_url, data={})
    response_json = response.json()
    barcodes = response_json["barcodes_group"]["barcodes"]
    return barcodes


def create_barcodes(num_required):
    # call Baracoda here and fetch a set of barcodes with the prefix we want
    print(f"Num barcodes required from Baracoda = {num_required}")
    list_barcodes = generate_baracoda_barcodes(num_required)
    print("list_barcodes = %s" % list_barcodes)
    return list_barcodes


def create_root_sample_id(barcode, well_num) -> str:
    return "RSID-%s%s" % (barcode, str(well_num).zfill(4))


def create_viral_prep_id(barcode, well_num, well_coordinate) -> str:
    return "VPID-%s%s_%s" % (barcode, str(well_num).zfill(4), well_coordinate)


def create_rna_id(barcode, well_coordinate) -> str:
    return "%s_%s" % (barcode, well_coordinate)


def create_rna_pcr_id(barcode, well_num, well_coordinate) -> str:
    return "RNA_PCR-%s%s_%s" % (barcode, str(well_num).zfill(4), well_coordinate)


def create_test_timestamp(dt) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def create_row(dt, well_index, result, barcode) -> str:
    well_coordinate = WELL_COORDS[well_index]
    well_num = well_index + 1
    return [
        create_root_sample_id(barcode, well_num),
        create_viral_prep_id(barcode, well_num, well_coordinate),
        create_rna_id(barcode, well_coordinate),
        create_rna_pcr_id(barcode, well_num, well_coordinate),
        result,
        create_test_timestamp(dt),
        "AP","","","","","","","","","","","",""
    ]


def create_plate_rows(dt, num_positives, plate_barcode):
    num_negatives = PLATE_SIZE - num_positives
    outcomes = ["Positive"] * num_positives + ["Negative"] * num_negatives
    random.shuffle(outcomes)

    return [create_row(dt, i, outcome, plate_barcode) for i, outcome in enumerate(outcomes)]


def create_csv_rows(plate_specs, dt, list_barcodes):
    # Turn [[2, 5], [3, 10]] into [5, 5, 10, 10, 10]
    pos_per_plate = flatten([[specs[1]] * specs[0] for specs in plate_specs])

    # Create a list of lists containing plate rows
    plate_rows = [create_plate_rows(dt, pos_per_plate[i], list_barcodes[i]) for i in range(len(pos_per_plate))]

    return flatten(plate_rows)
