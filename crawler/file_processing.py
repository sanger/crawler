import os
from typing import Dict, List, Any, Tuple
from pymongo.errors import BulkWriteError
from enum import Enum
from csv import DictReader, DictWriter
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
)
from crawler.helpers import current_time
from crawler.constants import (
    COLLECTION_SAMPLES,
    COLLECTION_SAMPLES_HISTORY,
)
from crawler.exceptions import CentreFileError
from crawler.db import (
    get_mongo_collection,
    get_mongo_db,
    create_mongo_client
)

from hashlib import md5

logger = logging.getLogger(__name__)

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
REGEX_FIELD = "sftp_file_regex"
ERRORS_DIR = 'errors'
SUCCESSES_DIR = 'successes'

class Centre:
    def __init__(self, config, centre_config):
      self.errors = []
      self.errors.clear()
      self.config = config
      self.centre_config = centre_config
      # TODO: check if sorted is oldest first
      self.centre_files = sorted(self.get_files_in_download_dir())

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
        logger.debug(f"Attempting to walk {path_to_walk}")
        (_, _, files) = next(os.walk(path_to_walk))

        pattern = re.compile(self.centre_config[REGEX_FIELD])

        # filter the list of files to only those which match the pattern
        centre_files = list(filter(pattern.match, files))

        return centre_files

    def clean_up(self) -> None:
        """Remove the files downloaded from the SFTP for the given centre.

        Arguments:
            centre {Dict[str, str]} -- the centre in question
        """
        logger.debug("Remove files")
        try:
            shutil.rmtree(get_download_dir())
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

        with get_sftp_connection(config) as sftp:
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
    # def __str__(self):
    #    return str(self)

    FILE_UNCHECKED = 1
    FILE_IN_BLACKLIST = 2
    FILE_NOT_PROCESSED_YET = 3
    FILE_PROCESSED_WITH_ERROR = 4
    FILE_PROCESSED_WITH_SUCCESS = 5

"""Class to process an individual file
"""
class CentreFile:
    def __init__(self, file_name, centre):
        self.errors = []
        self.errors.clear()

        self.centre = centre
        self.config = centre.config
        self.centre_config = centre.centre_config
        self.file_name = file_name
        self.file_state = CentreFileState.FILE_UNCHECKED


    def filepath(self):
        return PROJECT_ROOT.joinpath(f"{self.centre.get_download_dir()}{self.file_name}")

    def checksum():
        with open(self.filepath(), "rb") as f:
            file_hash = md5()
            while chunk := f.read(8192):
                file_hash.update(chunk)

        return file_hash.hexdigest()

# Volume structure:
# volume/centre1/errors
#               /successes
#       /centre2/errors
#               /successes

    def get_filename_with_checksum(self):
        return f"{self.file_name}_{checksum(self.file_name)}"

    def checksum_match(self, dir_path):
        return False

    def set_state_for_file(self) -> None:
        """Determines what state the file is in and whether it needs to be processed.
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

    def fetch_samples_with_field_value(self, field_name, field_value):
        return None

    def fetch_released_samples(self, samples):
        return None


    # def check_and_filter_out_released(self, samples) -> None:
    #     """Checks the list of samples in the file and filters out any that have already been released.

    #         Arguments:
    #             samples {?} -- a list of samples
    #     """
    #     root_sample_ids = map(lambda(x): x['Root Sample Id'], samples)
    #     fetch_samples_with_field_value('Root Sample Id', root_sample_ids)
    #     return None

    def archival_prepared_sample_conversor(self, sample, timestamp):
        sample['archived_at'] = timestamp
        sample['sample_object_id'] = sample['_id']
        del sample['_id']
        return sample

    def archival_prepared_samples(self, samples):
        timestamp = current_time()
        return list(map(lambda sample: self.archival_prepared_sample_conversor(sample, timestamp), samples))

    def archive_old_samples(self, samples_list) -> None:
        """Archives the old versions of samples we are updating so we retain a history.

            Arguments:
                samples {?} -- a list of samples
        """
        root_sample_ids = list(map(lambda x: x[FIELD_ROOT_SAMPLE_ID], samples_list))
        samples_collection_connector = get_mongo_collection(self.get_db(), COLLECTION_SAMPLES)
        existing_samples_to_archive = samples_collection_connector.find({FIELD_ROOT_SAMPLE_ID: {"$in": root_sample_ids}})

        samples_history_collection_connector = get_mongo_collection(self.get_db(), COLLECTION_SAMPLES_HISTORY)
        if existing_samples_to_archive.count() > 0:
            samples_history_collection_connector.insert_many(self.archival_prepared_samples(existing_samples_to_archive))

            assert existing_samples_to_archive.count() == len(root_sample_ids)

            samples_collection_connector.delete_many({FIELD_ROOT_SAMPLE_ID: {"$in": root_sample_ids}})

        return True

    def process_samples(self) -> None:
        """Processes the samples extracted from the centre file.
        """
        logger.info(f"Processing samples")

        parse_errors, docs_to_insert = self.parse_csv()
        if parse_errors:
            logger.info(f"Errors present in file {self.file_name}")
            # error handling - log error
            # write to volume 'errors'
            # add filename to blacklist?
            #continue
        else:
            logger.info(f"File valid")
            # TODO: if this is a new version of a file we already processed do we overwrite it?
            # or rename the previous one with a timestamp?
            # write to volume 'processed'

        self.insert_samples_from_docs(docs_to_insert)

    def get_db(self):
        client = create_mongo_client(self.config)
        db = get_mongo_db(self.config, client)
        return db

    def insert_samples_from_docs(self, docs_to_insert) -> None:
        """Insert sample records from the parsed file information.

        Arguments:
            docs_to_insert {List[Dict[str, str]]} -- list of sample information extracted from csv files
        """
        logger.debug(f"Attempting to insert {len(docs_to_insert)} docs")
        docs_inserted = 0
        client = create_mongo_client(self.config)
        db = get_mongo_db(self.config, client)
        samples_collection = get_mongo_collection(db, COLLECTION_SAMPLES)

        logger.info(docs_to_insert)


        try:
            # Moves previous version into history of samples
            self.archive_old_samples(docs_to_insert)

            # Inserts new version for samples
            result = samples_collection.insert_many(docs_to_insert, ordered=False)
            docs_inserted = len(result.inserted_ids)
        except BulkWriteError as e:
            # This is happening when there are duplicates in the data and the index prevents
            # the records from being written
            logger.warning(
                f"{e} - usually happens when duplicates are trying to be inserted"
            )
            docs_inserted = e.details["nInserted"]
            write_errors = {
                write_error["code"] for write_error in e.details["writeErrors"]
            }
            for error in write_errors:
                num_errors = len(
                    list(filter(lambda x: x["code"] == error, e.details["writeErrors"]))
                )
                self.errors.append(f"{num_errors} records with error code {error}")
        except Exception as e:
            self.errors.append(f"Critical error: {e}")
            #critical_errors += 1
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

            self.check_for_required_fields(csvreader)
            documents = self.add_extra_fields(csvreader)
        return self.errors, documents

    # TODO check why MK is passing given the below method
    # This is being called from parse_csv
    # TODO: Add validation for no unexpected headers (warning) - check with James
    def check_for_required_fields(self, csvreader: DictReader) -> None:
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

    def extract_fields(self, row: Dict[str, str], barcode_field: str, regex: str) -> Tuple[str, str]:
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

    def add_extra_fields(self, csvreader: DictReader) -> Tuple[List[str], List[Dict[str, str]]]:
        """Adds extra fields to the imported data which is required for querying.

        Arguments:
            csvreader {DictReader} -- CSV file reader to iterate over

        Returns:
            Tuple[List[str], List[Dict[str, str]]] -- list of errors and the augmented data
        """
        logger.debug("Adding extra fields")

        augmented_data = []
        barcode_mismatch = 0

        barcode_regex = self.centre_config["barcode_regex"]
        barcode_field = self.centre_config["barcode_field"]

        for row in csvreader:
            row["source"] = self.centre_config["name"]

            try:
                if row[barcode_field] and barcode_regex:
                    row[FIELD_PLATE_BARCODE], row[FIELD_COORDINATE] = self.extract_fields(
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
            self.errors.append(error)
            logger.warning(error)
            # TODO: Update regex check to handle different format checks
            #  https://ssg-confluence.internal.sanger.ac.uk/pages/viewpage.action?pageId=101358138#ReceiptfromLighthouselaboratories(Largediagnosticcentres)-4.2.1VariantsofRNAplatebarcode

        return augmented_data
