import os
from typing import Dict, List, Any, Tuple, Set
from pymongo.errors import BulkWriteError
from pymongo.database import Database
from enum import Enum
from csv import DictReader, DictWriter
import shutil
import logging, pathlib
import re
from crawler.constants import (
    FIELD_COORDINATE,
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_LINE_NUMBER,
    FIELD_FILE_NAME,
    FIELD_FILE_NAME_DATE,
    FIELD_CREATED_AT,
    FIELD_UPDATED_AT,
)
from crawler.helpers import current_time
from crawler.constants import (
    COLLECTION_SAMPLES,
    COLLECTION_SAMPLES_HISTORY,
    COLLECTION_IMPORTS,
)
from crawler.exceptions import CentreFileError
from crawler.db import (
    get_mongo_collection,
    get_mongo_db,
    create_mongo_client,
    create_import_record,
)

from hashlib import md5

import datetime

logger = logging.getLogger(__name__)

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
REGEX_FIELD = "sftp_file_regex"
ERRORS_DIR = "errors"
SUCCESSES_DIR = "successes"


class Centre:
    def __init__(self, config, centre_config):
        self.errors = []
        self.errors.clear()
        self.config = config
        self.centre_config = centre_config
        # TODO: check if sorted is oldest first
        self.centre_files = sorted(self.get_files_in_download_dir())

        # create backup directories for files
        os.makedirs(f"{self.centre_config['backups_folder']}/{ERRORS_DIR}", exist_ok=True)
        os.makedirs(f"{self.centre_config['backups_folder']}/{SUCCESSES_DIR}", exist_ok=True)

    def get_files_in_download_dir(self) -> List[str]:
        """Get all the files in the download directory for this centre and filter the file names using
        the regex described in the centre's 'regex_field'.

        Arguments:
            centre {Dict[str, str]} -- the centre in question
            regex_field {str} -- the field name where the regex is found to filter the files by

        Returns:
            List[str] -- all the file names in the download directory after filtering
        """
        logger.info(f"Fetching files of centre {self.centre_config['name']}")
        # get a list of files in the download directory
        # https://stackoverflow.com/a/3207973
        path_to_walk = PROJECT_ROOT.joinpath(self.get_download_dir())
        try:
            logger.debug(f"Attempting to walk {path_to_walk}")
            (_, _, files) = next(os.walk(path_to_walk))

            pattern = re.compile(self.centre_config[REGEX_FIELD])

            # filter the list of files to only those which match the pattern
            centre_files = list(filter(pattern.match, files))

            return centre_files
        except:
            logger.fatal(f"Failed when reading files from {path_to_walk}")
            return []

    def clean_up(self) -> None:
        """Remove the files downloaded from the SFTP for the given centre.

        Arguments:
            centre {Dict[str, str]} -- the centre in question
        """
        logger.debug("Remove files")
        try:
            shutil.rmtree(self.get_download_dir())
        except Exception as e:
            logger.exception("Failed clean up")

    def process_files(self) -> None:
        """Iterate through all the files for the centre, parsing any new ones into
        the database.
        """
        # iterate through each file in the centre

        for file_name in self.centre_files:
            logger.info(f"Checking file {file_name}")

            # create an instance of the file class to handle the file
            centre_file = CentreFile(file_name, self)

            centre_file.set_state_for_file()
            logger.debug(f"File state {CentreFileState[centre_file.file_state.name]}")

            # Process depending on file state
            if centre_file.file_state == CentreFileState.FILE_IN_BLACKLIST:
                # next file
                continue
            elif centre_file.file_state == CentreFileState.FILE_NOT_PROCESSED_YET:
                # process it
                centre_file.process_samples()
            elif centre_file.file_state == CentreFileState.FILE_PROCESSED_WITH_ERROR:
                # next file
                continue
            elif centre_file.file_state == CentreFileState.FILE_PROCESSED_WITH_SUCCESS:
                # next file
                continue
            else:
                # error unrecognised
                logger.info(f"Unrecognised file state {centre_file.file_state.name}")

    def get_download_dir(self) -> str:
        """Get the download directory where the files from the SFTP are stored.
        Returns:
            str -- the download directory
        """
        return f"{self.config.DIR_DOWNLOADED_DATA}{self.centre_config['prefix']}/"  # type: ignore

    def download_csv_files(self) -> None:
        """Downloads the centre's file from the SFTP server
        """
        logger.debug("Download CSV file(s) from SFTP")

        logger.debug("Create download directory for centre")
        try:
            os.mkdir(self.get_download_dir())
        except FileExistsError:
            pass

        with get_sftp_connection(self.config) as sftp:
            logger.debug("Connected to SFTP")
            logger.debug("Listing centre's root directory")
            logger.debug(f"ls: {sftp.listdir(self.centre_config['sftp_root_read'])}")

            # downloads all files
            logger.info("Downloading CSV files...")
            sftp.get_d(self.centre_config["sftp_root_read"], self.get_download_dir())

        return None


"""Class to hold enum states for the files
"""


class CentreFileState(Enum):
    FILE_UNCHECKED = 1
    FILE_IN_BLACKLIST = 2
    FILE_NOT_PROCESSED_YET = 3
    FILE_PROCESSED_WITH_ERROR = 4
    FILE_PROCESSED_WITH_SUCCESS = 5


"""Class to process an individual file
"""


class CentreFile:
    REQUIRED_FIELDS = {
        FIELD_ROOT_SAMPLE_ID,
        FIELD_RNA_ID,
        FIELD_RESULT,
        FIELD_DATE_TESTED,
    }

    def __init__(self, file_name, centre):
        """Initialiser for the class representing the file

            Arguments:
                centre {Dict[str][str]} -- the lighthouse centre
                file_name {str} - the file name of the file

            Returns:
                str -- the filepath for the file
        """
        self.errors = []
        self.errors.clear()
        self.critical_errors = 0

        self.centre = centre
        self.config = centre.config
        self.centre_config = centre.centre_config
        self.file_name = file_name
        self.file_state = CentreFileState.FILE_UNCHECKED

        self.docs_inserted = 0

    def filepath(self) -> str:
        """Returns the filepath for the file

            Returns:
                str -- the filepath for the file
        """
        return PROJECT_ROOT.joinpath(f"{self.centre.get_download_dir()}{self.file_name}")

    def checksum(self) -> str:
        """Returns the checksum for the file

            Returns:
                str -- the checksum for the file
        """
        with open(self.filepath(), "rb") as f:
            file_hash = md5()
            while chunk := f.read(8192):
                file_hash.update(chunk)

        return file_hash.hexdigest()

    def checksum_match(self, dir_path) -> str:
        """Checks a directory for a file matching the checksum of this file

            Arguments:
                dir_path {str} -> the directory path to be checked

            Returns:
                boolean -- whether the file matches or not
        """
        regexp = re.compile(r"^([\d]{6}_[\d]{4})_(.*)_(\w*)$")

        checksum_for_file = self.checksum()

        backup_folder = f"{self.centre_config['backups_folder']}/{dir_path}"
        files_from_backup_folder = os.listdir(backup_folder)
        for backup_filename in files_from_backup_folder:
            matches = regexp.match(backup_filename)
            if matches:
                backup_timestamp = matches.group(1)
                backup_filename = matches.group(2)
                backup_checksum = matches.group(3)

                if checksum_for_file == backup_checksum:
                    return True
        return False

    def set_state_for_file(self) -> CentreFileState:
        """Determines what state the file is in and whether it needs to be processed.

              Returns:
                  CentreFileState - enum representation of file state
        """
        # check whether file is on the blacklist and should be ignored
        if self.file_name in self.centre_config["file_names_to_ignore"]:
            self.file_state = CentreFileState.FILE_IN_BLACKLIST
            return

        # check whether file has already been processed to error directory
        if self.checksum_match(ERRORS_DIR):
            self.file_state = CentreFileState.FILE_PROCESSED_WITH_ERROR
            return

        # if checksum differs or file is not present in errors we check whether file has
        # already been processed successfully
        if self.checksum_match(SUCCESSES_DIR):
            self.file_state = CentreFileState.FILE_PROCESSED_WITH_SUCCESS
            return

        # if checksum differs or if the file was not present in success directory
        # we process it
        self.file_state = CentreFileState.FILE_NOT_PROCESSED_YET

    def archival_prepared_sample_conversor(self, sample, timestamp) -> Dict[str, str]:
        """Deletes the old sample and sets up the sample object for archiving

            Arguments:
                sample {Dict[str, str]} -- a sample that needs to be archived

            Returns:
                Dict[str, str] - the modified sample for archiving
        """
        sample["archived_at"] = timestamp
        sample["sample_object_id"] = sample["_id"]
        del sample["_id"]
        return sample

    # def archival_prepared_samples(self, samples) -> List[Dict[str, str]]:
    #     """Prepares the list of samples for archiving

    #         Arguments:
    #             samples_list {List[Dict[str, str]]} -- a list of samples

    #         Returns:
    #             List[Dict[str, str]] - the modified samples for archiving
    #     """
    #     timestamp = current_time()
    #     return list(
    #         map(lambda sample: self.archival_prepared_sample_conversor(sample, timestamp), samples)
    #     )

    # def archive_old_samples(self, samples_list) -> bool:
    #     """Archives the old versions of samples we are updating so we retain a history.

    #         Arguments:
    #             samples_list {List[Dict[str, str]]} -- a list of samples

    #         Returns:
    #             boolean - whether the samples were archived
    #     """
    #     root_sample_ids = list(map(lambda x: x[FIELD_ROOT_SAMPLE_ID], samples_list))
    #     samples_collection_connector = get_mongo_collection(self.get_db(), COLLECTION_SAMPLES)
    #     existing_samples_to_archive = samples_collection_connector.find(
    #         {FIELD_ROOT_SAMPLE_ID: {"$in": root_sample_ids}}
    #     )
    #     # TODO: Only archive a sample if there was any change by comparing with the the sample replacing

    #     samples_history_collection_connector = get_mongo_collection(
    #         self.get_db(), COLLECTION_SAMPLES_HISTORY
    #     )
    #     if existing_samples_to_archive.count() > 0:
    #         samples_history_collection_connector.insert_many(
    #             self.archival_prepared_samples(existing_samples_to_archive)
    #         )

    #         samples_collection_connector.delete_many(
    #             {FIELD_ROOT_SAMPLE_ID: {"$in": root_sample_ids}}
    #         )

    #     return True

    def process_samples(self) -> None:
        """Processes the samples extracted from the centre file.
        """
        logger.info(f"Processing samples")

        parse_errors, docs_to_insert = self.parse_csv()
        if parse_errors:
            logger.error(f"Errors present in file {self.file_name}")

        else:
            logger.info(f"File {self.file_name} is valid")

        self.insert_samples_from_docs(docs_to_insert)
        self.backup_file()
        self.create_import_record_for_file()

    def backup_filename(self) -> str:
        """Backup the file.

            Returns:
                str -- the filepath of the file backup
        """
        if len(self.errors) > 0:
            return (
                f"{self.centre_config['backups_folder']}/{ERRORS_DIR}/{self.timestamped_filename()}"
            )
        else:
            return f"{self.centre_config['backups_folder']}/{SUCCESSES_DIR}/{self.timestamped_filename()}"

    def timestamped_filename(self):
        return f"{current_time()}_{self.file_name}_{self.checksum()}"

    def full_path_to_file(self):
        return PROJECT_ROOT.joinpath(self.centre.get_download_dir(), self.file_name)

    def backup_file(self) -> str:
        """Backup the file.

            Returns:
                str -- destination of the file
        """
        destination = self.backup_filename()

        shutil.copyfile(self.full_path_to_file(), destination)

    def create_import_record_for_file(self):
        logger.info(f"{self.docs_inserted} documents inserted")

        # write status record
        imports_collection = get_mongo_collection(self.get_db(), COLLECTION_IMPORTS)
        _ = create_import_record(
            imports_collection, self.centre_config, self.docs_inserted, self.file_name, self.errors,
        )

    def get_db(self) -> Database:
        """Fetch the mongo database.

            Returns:
                Database -- a reference to the database in mongo
        """
        client = create_mongo_client(self.config)
        db = get_mongo_db(self.config, client)
        return db

    def add_duplication_errors(self, exception):
        try:
            wrong_instances = [
                write_error["op"] for write_error in exception.details["writeErrors"]
            ]

            for wrong_instance in wrong_instances:
                self.errors.append(
                    f"Duplicate insertion while inserting entry {wrong_instance['Root Sample ID']} from file {wrong_instance['file_name']} at line {wrong_instance['line_number']}"
                )
        except Exception:
            self.critical_errors += 1
            self.errors.append(f"Unknown error with file {self.file_name}")

    def insert_samples_from_docs(self, docs_to_insert) -> None:
        """Insert sample records from the parsed file information.

            Arguments:
                docs_to_insert {List[Dict[str, str]]} -- list of sample information extracted from csv files
        """
        logger.debug(f"Attempting to insert {len(docs_to_insert)} docs")
        samples_collection = get_mongo_collection(self.get_db(), COLLECTION_SAMPLES)

        try:
            # Moves previous version into history of samples
            # self.archive_old_samples(docs_to_insert)

            # Inserts new version for samples
            result = samples_collection.insert_many(docs_to_insert, ordered=False)
            self.docs_inserted = len(result.inserted_ids)
        except BulkWriteError as e:
            # This is happening when there are duplicates in the data and the index prevents
            # the records from being written
            logger.warning(f"{e} - usually happens when duplicates are trying to be inserted")
            self.docs_inserted = e.details["nInserted"]

            self.add_duplication_errors(e)
        except Exception as e:
            self.errors.append(f"Critical error in file {self.file_name}: {e}")
            self.critical_errors += 1
            logger.exception(e)

    def parse_csv(self) -> Tuple[List[str], List[Dict[str, str]]]:
        """Parses the CSV file of the centre.

        Returns:
            Tuple[List[str], List[str, str]] -- list of errors and the augmented data
        """
        csvfile_path = self.filepath()

        logger.info(f"Attempting to parse CSV file: {csvfile_path}")

        with open(csvfile_path, newline="") as csvfile:
            csvreader = DictReader(csvfile)

            self.check_for_required_headers(csvreader)
            documents = self.format_and_filter_rows(csvreader)

        return self.errors, documents

    def check_for_required_headers(self, csvreader: DictReader) -> None:
        """Checks that the CSV file has the required headers.

        Raises:
            CentreFileError: Raised when the required fields are not found in the file
        """
        logger.debug("Checking CSV for required headers")

        if csvreader.fieldnames:
            fieldnames = set(csvreader.fieldnames)
            if not self.REQUIRED_FIELDS <= fieldnames:
                raise CentreFileError(
                    f"{', '.join(list(self.REQUIRED_FIELDS - fieldnames))} missing in CSV file"
                )
        else:
            raise CentreFileError("Cannot read CSV fieldnames")

    def extract_fields(
        self, row: Dict[str, str], barcode_field: str, regex: str
    ) -> Tuple[str, str]:
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

    def get_now_timestamp(self):
        return datetime.datetime.now()

    def format_and_filter_rows(
        self, csvreader: DictReader
    ) -> Tuple[List[str], List[Dict[str, str]]]:
        """Adds extra fields to the imported data which are required for querying.

        Arguments:
            csvreader {DictReader} -- CSV file reader to iterate over

        Returns:
            Tuple[List[str], List[Dict[str, str]]] -- list of errors and the augmented data
        """
        logger.debug("Adding extra fields")

        augmented_data = []
        missing_data_count = 0
        invalid_rows = 0
        line_number = 1

        import_timestamp = self.get_now_timestamp()

        barcode_regex = self.centre_config["barcode_regex"]
        barcode_field = self.centre_config["barcode_field"]

        for row in csvreader:
            # only process rows that contain something in the cells
            if self.row_valid_structure(row, line_number):
                row["source"] = self.centre_config["name"]
                row[FIELD_PLATE_BARCODE] = None
                try:
                    if row[barcode_field] and barcode_regex:
                        row[FIELD_PLATE_BARCODE], row[FIELD_COORDINATE] = self.extract_fields(
                            row, barcode_field, barcode_regex
                        )

                except KeyError:
                    pass

                if row[FIELD_PLATE_BARCODE]:
                    row[FIELD_LINE_NUMBER] = line_number
                    row[FIELD_FILE_NAME] = self.file_name
                    row[FIELD_FILE_NAME_DATE] = self.file_name_date()
                    row[FIELD_CREATED_AT] = import_timestamp
                    row[FIELD_UPDATED_AT] = import_timestamp
                    augmented_data.append(row)
                else:
                    missing_data_count += 1

            else:
                invalid_rows += 1

            line_number += 1

        logger.info(f"Invalid rows = {invalid_rows}")

        if barcode_regex and missing_data_count > 0:
            error = f"{missing_data_count} sample rows are missing a plate barcode and / or a coordinate"
            self.errors.append(error)
            logger.warning(error)
            # TODO: Update regex check to handle different format checks
            #  https://ssg-confluence.internal.sanger.ac.uk/pages/viewpage.action?pageId=101358138#ReceiptfromLighthouselaboratories(Largediagnosticcentres)-4.2.1VariantsofRNAplatebarcode

        return augmented_data

    def row_valid_structure(self, row, line_number) -> bool:
        """Checks whether the row has the expected structure.
            Checks for blank rows and if we have the sample id and plate barcode.boolean

        Arguments:
            row {Row} - row object from the csvreader
            line_number - line number within the file

        Returns:
            bool -- whether the row has valid structure or not
        """
        # check whether row is completely empty (this is ok)
        if not (any(cell_txt.strip() for cell_txt in row.values())):
            return False

        # check both the Root Sample ID and barcode field are present
        barcode_field = self.centre_config["barcode_field"]

        if not (row[FIELD_ROOT_SAMPLE_ID] and row[barcode_field]):
            error = f"Row {line_number} has an empty plate barcode."
            self.errors.append(error)
            logger.error(error)
            return False

        return True

    def file_name_date(self) -> Any:
        """Extracts date from the filename if it matches the expected format.
            Otherwise returns None.

        Returns:
            datetime -- date extracted from the filename
        """
        # example filenames:
        # AP_sanger_report_200527_0818.csv
        # MK_sanger_report_200418_0800.csv
        # GLS_sanger_report_200529_2030.csv
        m = re.match(".*_([\d]{6}_[\d]{4})\.csv", self.file_name)

        if not m:
            return None

        file_timestamp = m.group(1)

        return datetime.datetime.strptime(file_timestamp, "%y%m%d_%H%M")
