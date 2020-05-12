import logging
import os
import pathlib
import re
from csv import DictReader, DictWriter
from datetime import datetime
from importlib import import_module
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple

import pysftp  # type: ignore

from crawler.constants import DIR_DOWNLOADED_DATA, FIELD_NAME_BARCODE, FIELD_NAME_COORDINATE
from crawler.exceptions import CentreFileError

logger = logging.getLogger(__name__)

PROJECT_ROOT = pathlib.Path(__file__).parent.parent


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


def download_csv_files(config, centre: Dict[str, str]) -> None:
    """Downloads the centre's file from the SFTP server

    Arguments:
        config {ModuleType} -- config which has the SFTP details
        centre {Dict[str, str]} -- centre details
    """
    logger.debug("Download CSV file(s) from SFTP")

    # disable host key checking https://bitbucket.org/dundeemt/pysftp/src/master/docs/cookbook.rst#rst-header-id5
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    with pysftp.Connection(
        host=config.SFTP_HOST,  # type: ignore
        port=config.SFTP_PORT,
        username=config.SFTP_READ_USER,
        password=config.SFTP_READ_PASSWORD,
        cnopts=cnopts,
    ) as sftp:
        logger.info("Connected to SFTP")
        logger.debug("Listing centre's root directory")
        logger.debug(f"ls: {sftp.listdir(centre['sftp_root_read'])}")

        # downloads all files
        logger.info("Downloading CSV files...")
        sftp.get_d(centre["sftp_root_read"], get_download_dir(centre))

    return None


def parse_csv(centre: Dict[str, str]) -> Tuple[str, List[str], List[Dict[str, str]]]:
    """Parses the CSV file of the centre.

    Arguments:
        centre {Dict[str, str]} -- centre details

    Returns:
        Tuple[str, List[str], List[str, str]] -- name of the file which was parsed for this centre,
        list of errors and the augmented data
    """
    if "merge_required" in centre.keys() and centre["merge_required"]:
        file_regex = "sftp_master_file_regex"
    else:
        file_regex = "sftp_file_regex"

    latest_file_name = get_latest_csv(centre, file_regex)

    csvfile_path = PROJECT_ROOT.joinpath(f"{get_download_dir(centre)}{latest_file_name}")

    logger.info(f"Attempting to parse CSV file: {csvfile_path}")

    with open(csvfile_path, newline="") as csvfile:
        csvreader = DictReader(csvfile)

        errors = check_for_required_fields(csvreader, centre)

        errors, documents = add_extra_fields(csvreader, centre, errors)

    return latest_file_name, errors, documents


def get_download_dir(centre: Dict[str, str]) -> str:
    download_dir = f"{DIR_DOWNLOADED_DATA}{centre['prefix']}/"

    return download_dir


def get_files_in_download_dir(centre: Dict[str, str], regex_field: str) -> List[str]:
    # get a list of files in the download directory
    # https://stackoverflow.com/a/3207973
    (_, _, files) = next(os.walk(PROJECT_ROOT.joinpath(get_download_dir(centre))))

    pattern = re.compile(centre[regex_field])

    # filter the list of files to only those which match the pattern
    centre_files = list(filter(pattern.match, files))

    return centre_files


def get_latest_csv(centre: Dict[str, str], regex_field: str) -> str:
    """Get the latest CSV file name for the centre which matches the regex described in the centre
    details file.

    Arguments:
        centre {Dict[str, str]} -- details of the centres

    Returns:
        str -- file name of the CSV file to parse
    """
    logger.debug(
        f"Getting latest CSV file for {centre['name']} using " f"pattern {centre[regex_field]}"
    )

    centre_files = get_files_in_download_dir(centre, regex_field)

    files_with_time = {}
    for filename in centre_files:
        if match := re.compile(centre[regex_field]).match(filename):
            files_with_time[datetime.strptime(match.group(1), "%y%m%d_%H%M")] = filename

    # return the latest one
    latest_file_name = files_with_time[sorted(files_with_time, reverse=True)[0]]
    logger.info(f"Latest file: {latest_file_name}")
    return latest_file_name


def merge_daily_files(centre: Dict[str, str]) -> str:
    logger.info(f"Merging daily files of {centre['name']}")

    # get list of files
    centre_files = get_files_in_download_dir(centre, "sftp_file_regex")
    logger.debug(f"{len(centre_files)} to merge into master file")

    # get the latest file name to use for the master name
    latest_file_name = get_latest_csv(centre, "sftp_file_regex")

    master_file_name = f"{latest_file_name[:-4]}_master.csv"
    with open(f"{get_download_dir(centre)}{master_file_name}", "w") as master_csv:
        field_names_written = False
        for filename in centre_files:
            logger.debug(f"Merging {filename} into {master_file_name}")
            with open(f"{get_download_dir(centre)}{filename}", "r", newline="") as daily_file:
                csvreader = DictReader(daily_file)

                if csvreader.fieldnames is None:
                    raise Exception("Field names required in CSV file")

                #  write header
                if not field_names_written:
                    writer = DictWriter(master_csv, fieldnames=csvreader.fieldnames)
                    writer.writeheader()
                    field_names_written = True

                # copy data
                for row in csvreader:
                    writer.writerow(row)

    logger.info(f"{master_file_name} created")

    return master_file_name


def upload_file_to_sftp(config: ModuleType, centre: Dict[str, str], filename: str) -> None:
    logger.info(f"Uploading {filename} to {centre['sftp_root_write']}")

    # disable host key checking https://bitbucket.org/dundeemt/pysftp/src/master/docs/cookbook.rst#rst-header-id5
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    sftp_host = config.SFTP_HOST  # type: ignore
    sftp_port = config.SFTP_PORT  # type: ignore
    sftp_user = config.SFTP_WRITE_USER  # type: ignore
    sftp_password = config.SFTP_WRITE_PASSWORD  # type: ignore

    with pysftp.Connection(
        host=sftp_host, port=sftp_port, username=sftp_user, password=sftp_password, cnopts=cnopts,
    ) as sftp:
        logger.info("Connected to SFTP")
        with sftp.cd(centre["sftp_root_write"]):
            # upload master file
            sftp.put(f"{get_download_dir(centre)}{filename}", confirm=True)


def clean_up(centre: Dict[str, str]) -> None:
    # remove up after running the jobs
    pass


def get_config(test_config: Dict[str, str] = None) -> Optional[ModuleType]:
    try:
        settings_module = os.environ["SETTINGS_MODULE"]

        logger.info(f"Using settings from {settings_module}")

        return import_module(settings_module)  # type: ignore
    except KeyError as e:
        logger.error(f"{e} required in environmental variables for config")

        return None
