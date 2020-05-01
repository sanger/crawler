import logging
import os
import pathlib
import re
from csv import DictReader
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pysftp  # type: ignore

from crawler.constants import DIR_DOWNLOADED_DATA, FIELD_NAME_BARCODE, FIELD_NAME_COORDINATE
from crawler.exceptions import CentreFileError

logger = logging.getLogger(__name__)


def extract_fields(row: Dict[str, str], barcode_field: str, regex: str) -> Tuple[str, str]:
    """Extracts fields from a row of data (from the CSV file). Currently extracting the barcode and
    coordinate (well position) using regex groups.

    Arguments:
        row {Dict[str, Any]} -- row of data from CSV file
        barcode_field {str} -- field indicating the plate barcode of interest, might also include
        coordinate
        regex {str} -- regex pattern to use to extract the fields

    Returns:
        Tuple[str, str] -- the barcode and coordinate
    """
    m = re.match(regex, row[barcode_field])

    if not m:
        return "", ""

    return m.group(1), m.group(2)


def add_extra_fields(
    csvreader: DictReader, centre: Dict[str, str], errors: List[str]
) -> Tuple[List[str], List[Dict[str, str]]]:
    """Adds extra fields to the imported data which is required for querying.

    Arguments:
        csvreader {DictReader} -- CSV file reader to iterate over
        centre {Dict[str, str]} -- centre details
        errors {List[str]} -- list of errors

    Returns:
        Tuple[List[str], List[Dict[str, str]]] -- list of errors and the augmented data
    """
    logger.debug("Adding extra fields")

    augmented_data = []
    errors = []

    barcode_mismatch = 0

    barcode_regex = centre["barcode_regex"]
    barcode_field = centre["barcode_field"]

    for row in csvreader:
        row["source"] = centre["name"]

        try:
            if row[barcode_field] and barcode_regex:
                row[FIELD_NAME_BARCODE], row[FIELD_NAME_COORDINATE] = extract_fields(
                    row, barcode_field, barcode_regex
                )
            else:
                row[FIELD_NAME_BARCODE] = row[barcode_field]

            if not row[FIELD_NAME_BARCODE]:
                barcode_mismatch += 1
        except KeyError:
            pass

        augmented_data.append(row)

    if barcode_regex and barcode_mismatch > 0:
        error = (
            f"{barcode_mismatch} sample barcodes did not match the regex: {barcode_regex} "
            f"for field '{barcode_field}'"
        )
        errors.append(error)
        logger.warning(error)

    return errors, augmented_data


def check_for_required_fields(csvreader: DictReader, centre: Dict[str, str]) -> List[str]:
    """Checks the data for any required fields and populates a list of errors if any are found.

    Arguments:
        csvreader {DictReader} -- CSV file reader to iterate over
        centre {Dict[str, str]} -- centre details

    Returns:
        List[str] -- list of errors experienced
    """
    logger.debug("Checking data for required fields")

    if csvreader.fieldnames and centre["barcode_field"] not in csvreader.fieldnames:
        error = "Barcode field not in CSV file"
        logger.error(error)
        return [error]

    return []


def download_csv(config: Dict[str, str], centre: Dict[str, str]) -> None:
    """Downloads the centre's file from the SFTP server

    Arguments:
        config {Dict[str, str]} -- config which has the SFTP details
        centre {Dict[str, str]} -- centre details
    """
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
        logger.debug(f"ls: {sftp.listdir(centre['sftp_root'])}")

        # downloads all files
        sftp.get_d(centre["sftp_root"], DIR_DOWNLOADED_DATA)


def parse_csv(centre: Dict[str, str]) -> Tuple[str, List[str], List[Dict[str, str]]]:
    """Parses the CSV file of the centre.

    Arguments:
        centre {Dict[str, str]} -- centre details

    Returns:
        Tuple[str, List[str], List[str, str]] -- name of the file which was parsed for this centre,
        list of errors and the augmented data
    """
    latest_file_name = get_latest_csv(centre)

    root = pathlib.Path(__file__).parent.parent
    csvfile_path = root.joinpath(f"{DIR_DOWNLOADED_DATA}{latest_file_name}")

    logger.info(f"Attempting to parse CSV file: {csvfile_path}")

    with open(csvfile_path, newline="") as csvfile:
        csvreader = DictReader(csvfile)

        errors = check_for_required_fields(csvreader, centre)

        errors, documents = add_extra_fields(csvreader, centre, errors)

    return latest_file_name, errors, documents


def get_latest_csv(centre_details: Dict[str, str]) -> str:
    """Get the latest CSV file for the centre which matches the regex described in the centre
    details file.

    Arguments:
        centre_details {Dict[str, str]} -- details of the centres

    Returns:
        str -- file name of the CSV file to parse
    """
    logger.debug(
        f"Getting latest CSV file for {centre_details['name']} using "
        f"pattern {centre_details['sftp_file_regex']}"
    )
    root = pathlib.Path(__file__).parent.parent

    # https://stackoverflow.com/a/3207973
    (_, _, files) = next(os.walk(root.joinpath(DIR_DOWNLOADED_DATA)))

    pattern = re.compile(centre_details["sftp_file_regex"])
    centre_files = filter(pattern.match, files)

    files_with_time = {}
    for filename in centre_files:
        if match := pattern.match(filename):
            files_with_time[datetime.strptime(match.group(1), "%y%m%d_%H%M")] = filename
        else:
            raise CentreFileError(
                f"'{filename}' does not match regex {centre_details['sftp_file_regex']}"
            )

    return files_with_time[sorted(files_with_time, reverse=True)[0]]
