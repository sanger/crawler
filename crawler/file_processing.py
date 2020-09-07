import os
import csv
from typing import Dict, List, Any, Tuple, Set
from pymongo.errors import BulkWriteError
from pymongo.database import Database
from bson.objectid import ObjectId

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
    FIELD_RESULT,
    FIELD_CREATED_AT,
    FIELD_UPDATED_AT,
    FIELD_VIRAL_PREP_ID,
    FIELD_RNA_PCR_ID,
)
from crawler.helpers import current_time, get_sftp_connection, LoggingCollection
from crawler.constants import (
    COLLECTION_SAMPLES,
    COLLECTION_SAMPLES_HISTORY,
    COLLECTION_IMPORTS,
    COLLECTION_CENTRES,
)
from crawler.exceptions import CentreFileError
from crawler.db import (
    get_mongo_collection,
    get_mongo_db,
    create_mongo_client,
    create_import_record,
    create_mysql_connection,
    run_mysql_many_insert_on_duplicate_query
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
        self.config = config
        self.centre_config = centre_config

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
            logger.error(f"Failed when reading files from {path_to_walk}")
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
        the mongo database and then into the unified warehouse.
        """
        # iterate through each file in the centre

        self.centre_files = sorted(self.get_files_in_download_dir())
        for file_name in self.centre_files:
            logger.info(f"Checking file {file_name}")

            # create an instance of the file class to handle the file
            centre_file = CentreFile(file_name, self)

            centre_file.set_state_for_file()
            logger.debug(f"File state {CentreFileState[centre_file.file_state.name]}")

            # Process depending on file state
            if centre_file.file_state == CentreFileState.FILE_IN_BLACKLIST:
                logger.info("File in blacklist, skipping")
                # next file
                continue
            elif centre_file.file_state == CentreFileState.FILE_NOT_PROCESSED_YET:
                # process it
                centre_file.process_samples()
            elif centre_file.file_state == CentreFileState.FILE_PROCESSED_WITH_ERROR:
                logger.info("File already processed as errored, skipping")
                # next file
                continue
            elif centre_file.file_state == CentreFileState.FILE_PROCESSED_WITH_SUCCESS:
                logger.info("File already processed successfully, skipping")
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
        logger.info("Downloading CSV file(s) from SFTP")

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
        FIELD_VIRAL_PREP_ID,
        FIELD_RNA_ID,
        FIELD_RNA_PCR_ID,
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
        self.logging_collection = LoggingCollection()

        self.centre = centre
        self.config = centre.config
        self.centre_config = centre.centre_config
        self.file_name = file_name
        self.file_state = CentreFileState.FILE_UNCHECKED

        self.docs_inserted = 0

    def filepath(self) -> Any:
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

    def checksum_match(self, dir_path) -> bool:
        """Checks a directory for a file matching the checksum of this file

            Arguments:
                dir_path {str} -> the directory path to be checked

            Returns:
                boolean -- whether the file matches or not
        """
        regexp = re.compile(r"^([\d]{6}_[\d]{4})_(.*)_(\w*)$")

        checksum_for_file = self.checksum()
        logger.debug(f"Checksum for file = {checksum_for_file}")

        backup_folder = f"{self.centre_config['backups_folder']}/{dir_path}"
        files_from_backup_folder = os.listdir(backup_folder)
        for backup_copy_file in files_from_backup_folder:
            matches = regexp.match(backup_copy_file)
            if matches:
                backup_timestamp = matches.group(1)
                backup_filename = matches.group(2)
                backup_checksum = matches.group(3)

                if checksum_for_file == backup_checksum:
                    if backup_filename != self.file_name:
                        logger.warning(
                            f"Found identical file {backup_filename} in path {dir_path} which has same checksum but different filename"
                        )
                    return True
        return False

    def get_centre_from_db(self):

        centre_collection = get_mongo_collection(self.get_db(), COLLECTION_CENTRES)
        return centre_collection.find({"name": self.centre_config["name"]})[0]

    def set_state_for_file(self) -> CentreFileState:
        """Determines what state the file is in and whether it needs to be processed.

              Returns:
                  CentreFileState - enum representation of file state
        """
        # check whether file is on the blacklist and should be ignored
        centre = self.get_centre_from_db()
        if "file_names_to_ignore" in centre:
            filenames_to_ignore = centre["file_names_to_ignore"]

            if self.file_name in filenames_to_ignore:
                self.file_state = CentreFileState.FILE_IN_BLACKLIST
                return self.file_state

        # check whether file has already been processed to error directory
        if self.checksum_match(ERRORS_DIR):
            self.file_state = CentreFileState.FILE_PROCESSED_WITH_ERROR
            return self.file_state

        # if checksum differs or file is not present in errors we check whether file has
        # already been processed successfully
        if self.checksum_match(SUCCESSES_DIR):
            self.file_state = CentreFileState.FILE_PROCESSED_WITH_SUCCESS
            return self.file_state

        # if checksum differs or if the file was not present in success directory
        # we process it
        self.file_state = CentreFileState.FILE_NOT_PROCESSED_YET
        return self.file_state

    def process_samples(self) -> None:
        """Processes the samples extracted from the centre file.
        """
        logger.info(f"Processing samples")

        # Internally traps TYPE 2: missing headers and TYPE 10 malformed files and returns docs_to_insert = []
        docs_to_insert = self.parse_csv()

        if self.logging_collection.get_count_of_all_errors_and_criticals() > 0:
            logger.error(f"Errors present in file {self.file_name}")
        else:
            logger.info(f"File {self.file_name} is valid")

        if len(docs_to_insert) > 0:
            mongo_ids = self.insert_samples_from_docs_into_mongo_db(docs_to_insert)
            # TODO: if critical error from mongo inserts, do we skip mlwh?
            # TODO: is it a good idea to do this here, or create a separate method in main.py to select from mongo between 2 timestamps?
            self.insert_samples_from_docs_into_mlwh(docs_to_insert, mongo_ids)

        self.backup_file()
        self.create_import_record_for_file()

    def backup_filename(self) -> str:
        """Backup the file.

            Returns:
                str -- the filepath of the file backup
        """
        if self.logging_collection.get_count_of_all_errors_and_criticals() > 0:
            return (
                f"{self.centre_config['backups_folder']}/{ERRORS_DIR}/{self.timestamped_filename()}"
            )
        else:
            return f"{self.centre_config['backups_folder']}/{SUCCESSES_DIR}/{self.timestamped_filename()}"

    def timestamped_filename(self):
        return f"{current_time()}_{self.file_name}_{self.checksum()}"

    def full_path_to_file(self):
        return PROJECT_ROOT.joinpath(self.centre.get_download_dir(), self.file_name)

    def backup_file(self) -> None:
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
            imports_collection,
            self.centre_config,
            self.docs_inserted,
            self.file_name,
            self.logging_collection.get_messages_for_import(),
        )

    def get_db(self) -> Database:
        """Fetch the mongo database.

            Returns:
                Database -- a reference to the database in mongo
        """
        client = create_mongo_client(self.config)
        db = get_mongo_db(self.config, client)
        return db

    # Database clash
    def add_duplication_errors(self, exception):
        try:
            wrong_instances = [
                write_error["op"] for write_error in exception.details["writeErrors"]
            ]
            samples_collection = get_mongo_collection(self.get_db(), COLLECTION_SAMPLES)
            for wrong_instance in wrong_instances:
                # To identify TYPE 7 we need to do a search for
                entry = samples_collection.find(
                    {
                        FIELD_ROOT_SAMPLE_ID: wrong_instance[FIELD_ROOT_SAMPLE_ID],
                        FIELD_RNA_ID: wrong_instance[FIELD_RNA_ID],
                        FIELD_RESULT: wrong_instance[FIELD_RESULT],
                        FIELD_LAB_ID: wrong_instance[FIELD_LAB_ID],
                    }
                )[0]
                if not (entry):
                    logger.critical(
                        f"When trying to insert root_sample_id: {wrong_instance[FIELD_ROOT_SAMPLE_ID]}, contents: {wrong_instance}"
                    )
                    continue

                if entry[FIELD_DATE_TESTED] != wrong_instance[FIELD_DATE_TESTED]:
                    self.logging_collection.add_error(
                        "TYPE 7",
                        f"Already in database, line: {wrong_instance['line_number']}, root sample id: {wrong_instance['Root Sample ID']}, dates: ({entry[FIELD_DATE_TESTED]} != {wrong_instance[FIELD_DATE_TESTED]})",
                    )
                else:
                    self.logging_collection.add_error(
                        "TYPE 6",
                        f"Already in database, line: {wrong_instance['line_number']}, root sample id: {wrong_instance['Root Sample ID']}",
                    )
        except Exception as e:
            logger.critical(f"Unknown error with file {self.file_name}: {e}")

    def insert_samples_from_docs_into_mongo_db(self, docs_to_insert) -> List[ObjectId]:
        """Insert sample records into the mongo database from the parsed file information.

            Arguments:
                docs_to_insert {List[Dict[str, str]]} -- list of filtered sample information extracted from csv files
        """
        logger.debug(f"Attempting to insert {len(docs_to_insert)} docs")
        samples_collection = get_mongo_collection(self.get_db(), COLLECTION_SAMPLES)

        try:
            # Inserts new version for samples
            result = samples_collection.insert_many(docs_to_insert, ordered=False)
            self.docs_inserted = len(result.inserted_ids)

            # inserted_ids is in the same order as docs_to_insert, even if the query has ordered=False parameter
            return result.inserted_ids

        # TODO could trap DuplicateKeyError specifically
        except BulkWriteError as e:
            # This is happening when there are duplicates in the data and the index prevents
            # the records from being written
            logger.warning(f"{e} - usually happens when duplicates are trying to be inserted")

            # filter out any errors that are duplicates by checking the code in e.details["writeErrors"]
            filtered_errors = list(filter(lambda x: x["code"] != 11000, e.details["writeErrors"]))

            if len(filtered_errors) > 0:
                logger.info(
                    f"Number of exceptions left after filtering out duplicates = {len(filtered_errors)}. Example:"
                )
                logger.info(filtered_errors[0])

            self.docs_inserted = e.details["nInserted"]
            self.add_duplication_errors(e)
        except Exception as e:
            logger.critical(f"Critical error in file {self.file_name}: {e}")
            logger.exception(e)

    def insert_samples_from_docs_into_mlwh(self, docs_to_insert, mongo_ids) -> None:
        """Insert sample records into the MLWH database from the parsed file information, including the corresponding mongodb _id

            Arguments:
                docs_to_insert {List[Dict[str, str]]} -- list of filtered sample information extracted from csv files

                mongo_ids {List[ObjectId]} -- list of mongodb ids in the same order as docs_to_insert, from the insert into the mongodb
        """

        # TODO: consider how to insert to MySQL from here in python, db configs for deployment project etc.

        # TODO: coguk barcode blank at this point for inserts

        # TODO: SQL like this, will insert or update if keys match:
        # INSERT INTO table (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE a=VALUES(a), b=VALUES(b), c=VALUES(c);
        # TODO: consider splitting into batches to avoid hitting MySQL maximum_packet_size limitation

        # TODO: consider error handling, if any row in the batch insert fails, done in transaction so all fail.
        # TODO: plus then how to re-run them? And how to run for legacy data?

        values = []
        for i, doc in enumerate(docs_to_insert):
            mongo_id = mongo_ids[i]
            values.append(self.map_mongo_to_sql_columns(doc, mongo_id))

        mysql_conn = create_mysql_connection(self.config)

        run_mysql_many_insert_on_duplicate_query(mysql_conn, values)

    def map_mongo_to_sql_columns(self, doc, mongo_id) -> Dict[str, Any]:
        """Transform the record from using the mongodb field names into a form suitable for the MLWH
           We are not setting created_at_external and updated_at_external fields here
           because it would be slow to retrieve them from MongoDB
           and they would be virtually the same as created_at and updated_at

            Arguments:
                doc {Dict[str, str]} -- filtered information about one sample, extracted from csv files

                mongo_id {ObjectId} -- mongodb id from the insert of this sample into the mongodb
        """
        return {
            'mongodb_id': str(mongo_id), # hexadecimal string representation of BSON ObjectId. Do ObjectId(hex_string) to turn it back
            'root_sample_id': doc['Root Sample ID'],
            'rna_id': doc['RNA ID'],
            'plate_barcode': doc['plate_barcode'],
            'coordinate': doc['coordinate'],
            'result': doc['Result'],
            'date_tested_string': doc['Date Tested'],
            'date_tested': self.parse_date_tested(self, doc['Date Tested']),
            'source': doc['source'],
            'lab_id': doc['Lab ID'],
            'created_at': datetime.datetime.now,
            'updated_at': datetime.datetime.now
        }

    def parse_date_tested(self, date_string) -> datetime.datetime:
        format = '%Y-%m-%d %H:%M:%S %Z'
        date_time = datetime.datetime.strptime(date_string, format)
        if date_string.find('UTC') != -1:
            # timezone doesn't get set despite the '%Z' in the format string, unless we do this
            date_time = date_time.replace(tzinfo=datetime.timezone.utc)
        return date_time

    def parse_csv(self) -> List[Dict[str, Any]]:
        """Parses the CSV file of the centre.

        Returns:
            List[str, str] -- the augmented data
        """
        csvfile_path = self.filepath()

        logger.info(f"Attempting to parse CSV file: {csvfile_path}")

        with open(csvfile_path, newline="") as csvfile:
            csvreader = DictReader(csvfile)
            try:
                if self.check_for_required_headers(csvreader):
                    documents = self.format_and_filter_rows(csvreader)

                    return documents
            except csv.Error as e:
                self.logging_collection.add_error("TYPE 10", f"Wrong read from file")

        return []

    def get_required_headers(self) -> Set[str]:
        """Determines the required headers. Includes lab id if config flag is set.

            Returns:
                [str] - array of headers
        """
        required = set(self.REQUIRED_FIELDS)
        if not (self.config.ADD_LAB_ID):
            required.add(FIELD_LAB_ID)

        return required

    def check_for_required_headers(self, csvreader: DictReader) -> bool:
        """Checks that the CSV file has the required headers.

        Raises:
            CentreFileError: Raised when the required fields are not found in the file
        """
        logger.debug("Checking CSV for required headers")

        if csvreader.fieldnames:
            fieldnames = set(csvreader.fieldnames)
            required = self.get_required_headers()

            if not required <= fieldnames:
                # LOG_HANDLER TYPE 2: Fail file
                self.logging_collection.add_error(
                    "TYPE 2",
                    f"Wrong headers, {', '.join(list(required - fieldnames))} missing in CSV file",
                )
                return False
        else:
            self.logging_collection.add_error("TYPE 2", "Cannot read CSV fieldnames")
            return False

        return True

    def extract_fields(
        self, row: Dict[str, str], line_number, barcode_field: str, regex: str
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
        # TODO: Update regex check to handle different format checks
        #  https://ssg-confluence.internal.sanger.ac.uk/pages/viewpage.action?pageId=101358138#ReceiptfromLighthouselaboratories(Largediagnosticcentres)-4.2.1VariantsofRNAplatebarcode

        if not m:
            sample_id = None
            if FIELD_ROOT_SAMPLE_ID in row:
                sample_id = row[FIELD_ROOT_SAMPLE_ID]
            self.logging_collection.add_error(
                "TYPE 9",
                f"Wrong reg. exp. {barcode_field}, line:{line_number}, root_sample_id: {sample_id}, value: {row[barcode_field]}",
            )
            return "", ""

        return m.group(1), m.group(2)

    def get_now_timestamp(self):
        return datetime.datetime.now()

    def get_row_signature(self, row):
        memo = []
        for key in [FIELD_ROOT_SAMPLE_ID, FIELD_RNA_ID, FIELD_RESULT, FIELD_LAB_ID]:
            if key in row:
                memo.append(row[key])
        return tuple(memo)

    def log_adding_default_lab_id(self, row, line_number):
        logger.debug(f"Adding in missing Lab ID for row {line_number}")
        self.logging_collection.add_error(
            "TYPE 12",
            f"No Lab ID, line: {line_number}, root_sample_id: {row[FIELD_ROOT_SAMPLE_ID]}",
        )

    def filtered_row(self, row, line_number) -> Dict[str, str]:
        """ Filter unneeded columns and add lab id if not present and config flag set.

            Arguments:
                row {Dict[str][str]} - sample row read from file

            Returns:
                Dict[str][str] - returns a modified version of the row
        """
        modified_row: Dict[str, str] = {}
        if self.config.ADD_LAB_ID:
            # when we need to add the lab id if not present
            if FIELD_LAB_ID in row:
                # if the lab id field is already present
                if row[FIELD_LAB_ID] == "" or row[FIELD_LAB_ID] == None:
                    # if no value we add the default value and log it was missing
                    modified_row[FIELD_LAB_ID] = self.centre_config["lab_id_default"]
                    self.log_adding_default_lab_id(row, line_number)
                else:
                    if row[FIELD_LAB_ID] != self.centre_config["lab_id_default"]:
                        logger.warning(
                            f"Different lab id setting: {row[FIELD_LAB_ID]}!={self.centre_config['lab_id_default']}"
                        )
                    modified_row[FIELD_LAB_ID] = row[FIELD_LAB_ID]
            else:
                # if the lab id field is not present we add the default and log it was missing
                modified_row[FIELD_LAB_ID] = self.centre_config["lab_id_default"]
                self.log_adding_default_lab_id(row, line_number)

        # filter out any unexpected columns
        for key in self.get_required_headers():
            if key in row:
                modified_row[key] = row[key]

        unexpected_headers = list(row.keys() - modified_row.keys())
        if len(unexpected_headers) > 0:
            self.logging_collection.add_error(
                "TYPE 13",
                f"Unexpected headers, line: {line_number}, root_sample_id: {row[FIELD_ROOT_SAMPLE_ID]}, extra headers: {unexpected_headers}",
            )

        return modified_row

    def format_and_filter_rows(self, csvreader: DictReader) -> Any:
        """Adds extra fields to the imported data which are required for querying.

        Arguments:
            csvreader {DictReader} -- CSV file reader to iterate over

        Returns:
            Tuple[List[str], List[Dict[str, str]]] -- list of errors and the augmented data
        """
        logger.debug("Adding extra fields")

        augmented_data = []

        # Detect duplications and filters them out
        seen_rows: Set[tuple] = set()
        missing_data_count = 0
        invalid_rows_count = 0
        line_number = 2

        import_timestamp = self.get_now_timestamp()

        barcode_regex = self.centre_config["barcode_regex"]
        barcode_field = self.centre_config["barcode_field"]

        for row in csvreader:
            # only process rows that contain something in the cells
            if self.row_valid_structure(row, line_number):
                row = self.filtered_row(row, line_number)
                row["source"] = self.centre_config["name"]
                row[FIELD_PLATE_BARCODE] = None  # type: ignore

                if row[barcode_field] and barcode_regex:
                    row[FIELD_PLATE_BARCODE], row[FIELD_COORDINATE] = self.extract_fields(
                        row, line_number, barcode_field, barcode_regex
                    )

                if row[FIELD_PLATE_BARCODE]:
                    row[FIELD_LINE_NUMBER] = line_number  # type: ignore
                    row[FIELD_FILE_NAME] = self.file_name
                    row[FIELD_FILE_NAME_DATE] = self.file_name_date()
                    row[FIELD_CREATED_AT] = import_timestamp
                    row[FIELD_UPDATED_AT] = import_timestamp

                    row_signature = self.get_row_signature(row)

                    if row_signature in seen_rows:
                        logger.debug(f"Skipping {row_signature}: duplicate")
                        self.logging_collection.add_error(
                            "TYPE 5",
                            f"Duplicated, line: {line_number}, root_sample_id: {row[FIELD_ROOT_SAMPLE_ID]}",
                        )
                        continue
                    seen_rows.add(row_signature)
                    augmented_data.append(row)
                else:
                    missing_data_count += 1

            else:
                invalid_rows_count += 1

            line_number += 1

        logger.info(f"Incorrect rows in this file = {invalid_rows_count + missing_data_count}")

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
            self.logging_collection.add_error("TYPE 1", f"Empty line, line: {line_number}")
            return False

        # check both the Root Sample ID and barcode field are present
        barcode_field = self.centre_config["barcode_field"]
        if not (row[FIELD_ROOT_SAMPLE_ID]):
            # filter out row as sample id is missing
            self.logging_collection.add_error(
                "TYPE 3", f"Root Sample ID missing, line: {line_number}"
            )
            logger.warning(f"We found line: {line_number} missing sample id but is not blank")
            return False

        if not (row[FIELD_RESULT]):
            # filter out row as result is missing
            self.logging_collection.add_error("TYPE 3", f"Result missing, line: {line_number}")
            return False

        if not (row[barcode_field]):
            # filter out row as barcode is missing
            self.logging_collection.add_error("TYPE 4", f"RNA ID missing, line: {line_number}")
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
        m = re.match(r".*_([\d]{6}_[\d]{4})\.csv", self.file_name)

        if not m:
            return None

        file_timestamp = m.group(1)

        return datetime.datetime.strptime(file_timestamp, "%y%m%d_%H%M")
