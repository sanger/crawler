import logging
import os
import pathlib
import re
import shutil
import sys
from csv import DictReader, DictWriter
from datetime import datetime
from importlib import import_module
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple

import pysftp  # type: ignore

from crawler.constants import (
    FIELD_COORDINATE,
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
)
from crawler.exceptions import CentreFileError

logger = logging.getLogger(__name__)

PROJECT_ROOT = pathlib.Path(__file__).parent.parent


def current_time() -> str:
    """Generates a String containing a current timestamp in the format
    yymmdd_hhmm
    eg. 12:30 1st February 2019 becomes 190201_1230

    Returns:
        str -- A string with the current timestamp
    """
    return datetime.now().strftime("%y%m%d_%H%M")


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
    csvreader: DictReader, centre: Dict[str, str], errors: List[str] = []
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
    barcode_mismatch = 0

    barcode_regex = centre["barcode_regex"]
    barcode_field = centre["barcode_field"]

    for row in csvreader:
        row["source"] = centre["name"]

        try:
            if row[barcode_field] and barcode_regex:
                row[FIELD_PLATE_BARCODE], row[FIELD_COORDINATE] = extract_fields(
                    row, barcode_field, barcode_regex
                )
            else:
                row[FIELD_PLATE_BARCODE] = row[barcode_field]

            if not row[FIELD_PLATE_BARCODE]:
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
        # TODO: Update regex check to handle different format checks
        #  https://ssg-confluence.internal.sanger.ac.uk/pages/viewpage.action?pageId=101358138#ReceiptfromLighthouselaboratories(Largediagnosticcentres)-4.2.1VariantsofRNAplatebarcode

    return errors, augmented_data


# TODO check why MK is passing given the below method
# This is being called from parse_csv
# TODO: Add validation for no unexpected headers (warning) - check with James
def check_for_required_fields(csvreader: DictReader, centre: Dict[str, str]) -> None:
    """Checks that the CSV file has the required headers.

    Raises:
        CentreFileError: Raised when the required fields are not found in the file
    """
    logger.debug("Checking CSV for required headers")
    required_fields = {
        FIELD_ROOT_SAMPLE_ID,
        FIELD_RNA_ID,
        FIELD_RESULT,
        FIELD_DATE_TESTED,
    }
    if csvreader.fieldnames:
        fieldnames = set(csvreader.fieldnames)
        if not required_fields <= fieldnames:
            raise CentreFileError(
                f"{', '.join(list(required_fields - fieldnames))} missing in CSV file"
            )
    else:
        raise CentreFileError("Cannot read CSV fieldnames")


def download_csv_files(config, centre: Dict[str, str]) -> None:
    """Downloads the centre's file from the SFTP server

    Arguments:
        config {ModuleType} -- config which has the SFTP details
        centre {Dict[str, str]} -- centre details
    """
    logger.debug("Download CSV file(s) from SFTP")

    logger.debug("Create download directory for centre")
    try:
        os.mkdir(get_download_dir(config, centre))
    except FileExistsError:
        pass

    with get_sftp_connection(config) as sftp:
        logger.debug("Connected to SFTP")
        logger.debug("Listing centre's root directory")
        logger.debug(f"ls: {sftp.listdir(centre['sftp_root_read'])}")

        # downloads all files
        logger.info("Downloading CSV files...")
        sftp.get_d(centre["sftp_root_read"], get_download_dir(config, centre))

    return None

def parse_csv(
    config: ModuleType, centre: Dict[str, str], file_name: str
) -> Tuple[List[str], List[Dict[str, str]]]:
    """Parses the CSV file of the centre.

    Arguments:
        config {ModuleType} -- app config
        centre {Dict[str, str]} -- centre details
        file_name {str} -- file name to parse

    Returns:
        Tuple[List[str], List[str, str]] -- list of errors and the augmented data
    """
    csvfile_path = PROJECT_ROOT.joinpath(f"{get_download_dir(config, centre)}{file_name}")

    logger.info(f"Attempting to parse CSV file: {csvfile_path}")

    with open(csvfile_path, newline="") as csvfile:
        csvreader = DictReader(csvfile)

        check_for_required_fields(csvreader, centre)
        errors, documents = add_extra_fields(csvreader, centre)

    return errors, documents

def get_download_dir(config: ModuleType, centre: Dict[str, str]) -> str:
    """Get the download directory where the files from the SFTP are stored.

    Arguments:
        centre {Dict[str, str]} -- the centre in question

    Returns:
        str -- the download directory
    """
    return f"{config.DIR_DOWNLOADED_DATA}{centre['prefix']}/"  # type: ignore


def get_files_in_download_dir(
    config: ModuleType, centre: Dict[str, str], regex_field: str
) -> List[str]:
    """Get all the files in the download directory for this centre and filter the file names using
    the regex described in the centre's 'regex_field'.

    Arguments:
        centre {Dict[str, str]} -- the centre in question
        regex_field {str} -- the field name where the regex is found to filter the files by

    Returns:
        List[str] -- all the file names in the download directory after filtering
    """
    # get a list of files in the download directory
    # https://stackoverflow.com/a/3207973
    path_to_walk = PROJECT_ROOT.joinpath(get_download_dir(config, centre))
    logger.debug(f"Attempting to walk {path_to_walk}")
    (_, _, files) = next(os.walk(path_to_walk))

    pattern = re.compile(centre[regex_field])

    # filter the list of files to only those which match the pattern
    centre_files = list(filter(pattern.match, files))

    return centre_files


def get_latest_csv(config: ModuleType, centre: Dict[str, str], regex_field: str) -> str:
    """Get the latest CSV file name for the centre which matches the regex described by the
    'regex_field' for the centre.

    Arguments:
        centre {Dict[str, str]} -- the centre in question

    Returns:
        str -- file name of the CSV file to parse
    """
    logger.debug(
        f"Getting latest CSV file for {centre['name']} using " f"pattern {centre[regex_field]}"
    )

    centre_files = get_files_in_download_dir(config, centre, regex_field)

    pattern = re.compile(centre[regex_field])
    files_with_time = {}

    for filename in centre_files:
        if match := pattern.match(filename):
            files_with_time[datetime.strptime(match.group(1), "%y%m%d_%H%M")] = filename

    # return the latest one
    latest_file_name = files_with_time[sorted(files_with_time, reverse=True)[0]]
    logger.debug(f"Latest file: {latest_file_name}")

    return latest_file_name


def merge_daily_files(config: ModuleType, centre: Dict[str, str]) -> str:
    """Merge all the daily incremental files of the centre into one 'master' file. The master
    file's name is created by appending '_master' to the latest CSV file name.
    Any files pre-dating the merge_start_date option in the centre configuration
    will be excluded from the merge.

    Arguments:
        centre {Dict[str, str]} -- the centre in question

    Raises:
        CentreFileError: raised when no field names are found in the CSV file

    Returns:
        str -- the name of the master file created
    """
    logger.info(f"Merging daily files of {centre['name']}")

    # get list of files
    centre_files = get_files_in_download_dir(config, centre, "sftp_file_regex")
    logger.info(f"{len(centre_files)} files to merge into master file")

    # get the latest file name to use for the master name
    latest_file_name = get_latest_csv(config, centre, "sftp_file_regex")
    pattern = re.compile(centre["sftp_file_regex"])

    # TODO: Add valdiation to check extension
    # that latest_file_name.split('.')[1] == .csv
    master_file_name = f"{latest_file_name[:-4]}_master.csv"
    with open(f"{get_download_dir(config, centre)}{master_file_name}", "w") as master_csv:
        field_names_written = False

        # There is slight overlap in data on the transition from complete dumps
        # to incremental updates. We use a set to keep track of rows we've seen
        # so that we may filter them
        seen_rows = set()

        for filename in sorted(centre_files):
            if filename in centre["file_names_to_ignore"]:
                continue

            # Ignore files which predate the merge_start_date if specified
            if (match := pattern.match(filename)) and "merge_start_date" in centre.keys():
                file_date = datetime.strptime(match.group(1), "%y%m%d_%H%M")
                start_date = datetime.strptime(centre["merge_start_date"], "%y%m%d")
                if file_date < start_date:
                    logger.debug(f"Skipping {filename} as predates start_date")
                    continue

            logger.debug(f"Merging {filename} into {master_file_name}")
            with open(
                f"{get_download_dir(config, centre)}{filename}", "r", newline=""
            ) as daily_file:
                csvreader = DictReader(daily_file)

                if csvreader.fieldnames is None:
                    raise CentreFileError("Field names required in CSV file")

                #  write header
                if not field_names_written:
                    writer = DictWriter(
                        master_csv, fieldnames=csvreader.fieldnames, extrasaction="ignore"
                    )
                    writer.writeheader()
                    field_names_written = True

                # copy data
                for row in csvreader:
                    # Convert the row into a tuple so that we may store it in a
                    # set. Rows will only be ignored if completely identical.
                    row_signature = tuple(row.items())
                    if row_signature in seen_rows:
                        logger.debug(f"Skipping {row_signature}: duplicate")
                        continue
                    seen_rows.add(row_signature)
                    writer.writerow(row)

    logger.info(f"{master_file_name} created")

    return master_file_name


def upload_file_to_sftp(config: ModuleType, centre: Dict[str, str], filename: str) -> None:
    """Uploads the given file to the centre's SFTP write folder.

    Arguments:
        config {ModuleType} -- app config
        centre {Dict[str, str]} -- the centre in question
        filename {str} -- filename of file to be uploaded
    """
    logger.debug(f"Attempting upload of {filename} to {centre['sftp_root_write']}")

    sftp_write_username = config.SFTP_WRITE_USERNAME  # type: ignore
    sftp_write_password = config.SFTP_WRITE_PASSWORD  # type: ignore
    with get_sftp_connection(config, sftp_write_username, sftp_write_password) as sftp:
        logger.info("Connected to SFTP")
        with sftp.cd(centre["sftp_root_write"]):
            logger.info(f"Uploading {filename} to {centre['sftp_root_write']}")
            sftp.put(f"{get_download_dir(config, centre)}{filename}", confirm=True)


def get_sftp_connection(
    config: ModuleType, username: str = None, password: str = None
) -> pysftp.Connection:
    """Get a connection to the SFTP server as a context manager. The READ credentials are used by
    default but a username and password provided will override these.

    Arguments:
        config {ModuleType} -- application config

    Keyword Arguments:
        username {str} -- username to use instead of the READ username (default: {None})
        password {str} -- password for the provided username (default: {None})

    Returns:
        pysftp.Connection -- a connection to the SFTP server as a context manager
    """
    # disable host key checking:
    #   https://bitbucket.org/dundeemt/pysftp/src/master/docs/cookbook.rst#rst-header-id5
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    sftp_host = config.SFTP_HOST  # type: ignore
    sftp_port = config.SFTP_PORT  # type: ignore
    sftp_username = config.SFTP_READ_USERNAME if username is None else username  # type: ignore
    sftp_password = config.SFTP_READ_PASSWORD if username is None else password  # type: ignore

    return pysftp.Connection(
        host=sftp_host,
        port=sftp_port,
        username=sftp_username,
        password=sftp_password,
        cnopts=cnopts,
    )


def clean_up(config, centre: Dict[str, str]) -> None:
    """Remove the files downloaded from the SFTP for the given centre.

    Arguments:
        centre {Dict[str, str]} -- the centre in question
    """
    logger.debug("Remove files")
    try:
        shutil.rmtree(get_download_dir(config, centre))
    except Exception as e:
        logger.exception("Failed clean up")


def get_config(settings_module: str) -> Tuple[ModuleType, str]:
    """Get the config for the app by importing a module named by an environmental variable. This
    allows easy switching between environments and inheriting default config values.

    Arguments:
        settings_module {str} -- the settings module to load

    Returns:
        Optional[ModuleType] -- the config module loaded and available to use via `config.<param>`
    """
    try:
        if not settings_module:
            settings_module = os.environ["SETTINGS_MODULE"]

        return import_module(settings_module), settings_module  # type: ignore
    except KeyError as e:
        sys.exit(f"{e} required in environmental variables for config")
