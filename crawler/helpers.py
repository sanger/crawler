import logging
import pathlib
import re
from csv import DictReader
from typing import Dict, List, Tuple

import pysftp  # type: ignore

from crawler.constants import DIR_DOWNLOADED_DATA

logger = logging.getLogger(__name__)


def extract_plate_barcode(row: Dict, barcode_field: str, barcode_regex: str) -> str:
    m = re.match(barcode_regex, row[barcode_field])

    if not m:
        return ""

    return m.group(1)


def add_extra_fields(csvreader: DictReader, centre: Dict, errors: List) -> Tuple[List, List]:
    logger.debug("Adding extra fields")

    augmented_data = []
    errors = []

    barcode_mismatch = 0

    barcode_regex = centre["barcode_regex"]
    barcode_field = centre["barcode_field"]

    for row in csvreader:
        row["source"] = centre["name"]

        try:
            if barcode_regex:
                row["plate_barcode"] = extract_plate_barcode(row, barcode_field, barcode_regex)
            else:
                row["plate_barcode"] = row[barcode_field]

            if row["plate_barcode"] == "":
                barcode_mismatch += 1
        except KeyError:
            pass

        augmented_data.append(row)

    if barcode_regex and barcode_mismatch > 0:
        error = f"{barcode_mismatch} sample barcodes did not match the regex: {barcode_regex}"
        errors.append(error)
        logger.error(error)

    return errors, augmented_data


def check_for_required_fields(csvreader: DictReader, centre: Dict) -> List:
    logger.debug("Checking for required fields")

    if csvreader.fieldnames and centre["barcode_field"] not in csvreader.fieldnames:
        error = "Barcode field not in CSV file"
        logger.error(error)
        return [error]

    return []


def download_csv(config: Dict, centre: Dict) -> None:
    logger.debug("Download CSV file from SFTP")

    # disable host key checking https://bitbucket.org/dundeemt/pysftp/src/master/docs/cookbook.rst#rst-header-id5
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    with pysftp.Connection(
        host=config["SFTP_HOST"],
        port=int(config["SFTP_PORT"]),
        username=config["SFTP_USER"],
        password=config["SFTP_PASSWORD"],
        cnopts=cnopts,
    ) as sftp:
        logger.debug("Connected to SFTP")
        logger.debug(f"ls: {sftp.listdir()}")

        # save CSV file(s) to "data" dir
        sftp.get_d(centre["sftp_root"], DIR_DOWNLOADED_DATA)


def parse_csv(centre: Dict) -> Tuple[List, List]:
    root = pathlib.Path(__file__).parent.parent
    csvfile_path = root.joinpath(f"{DIR_DOWNLOADED_DATA}{centre['sftp_file_name']}")

    logger.info(f"Attempting to parse CSV file: {csvfile_path}")

    with open(csvfile_path, newline="") as csvfile:
        csvreader = DictReader(csvfile)

        errors = check_for_required_fields(csvreader, centre)

        return add_extra_fields(csvreader, centre, errors)
