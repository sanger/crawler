import csv
import logging
import os
import pathlib
import re
import shutil
import uuid
from csv import DictReader
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import md5
from logging import INFO, WARN
from pathlib import Path
from typing import Any, Dict, Final, List, Optional, Set, Tuple, cast

from bson.decimal128 import Decimal128
from more_itertools import groupby_transform
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from crawler.constants import (
    ALLOWED_CH_RESULT_VALUES,
    ALLOWED_CH_TARGET_VALUES,
    ALLOWED_RESULT_VALUES,
    COLLECTION_CENTRES,
    COLLECTION_IMPORTS,
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    DART_STATE_PENDING,
    FIELD_BARCODE,
    FIELD_CH1_CQ,
    FIELD_CH1_RESULT,
    FIELD_CH1_TARGET,
    FIELD_CH2_CQ,
    FIELD_CH2_RESULT,
    FIELD_CH2_TARGET,
    FIELD_CH3_CQ,
    FIELD_CH3_RESULT,
    FIELD_CH3_TARGET,
    FIELD_CH4_CQ,
    FIELD_CH4_RESULT,
    FIELD_CH4_TARGET,
    FIELD_COORDINATE,
    FIELD_CREATED_AT,
    FIELD_DATE_TESTED,
    FIELD_FILE_NAME,
    FIELD_FILE_NAME_DATE,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_LAB_ID,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_LINE_NUMBER,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_RNA_PCR_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SOURCE,
    FIELD_UPDATED_AT,
    FIELD_VIRAL_PREP_ID,
    MAX_CQ_VALUE,
    MIN_CQ_VALUE,
    POSITIVE_RESULT_VALUE,
)
from crawler.db.dart import (
    add_dart_plate_if_doesnt_exist,
    add_dart_well_properties_if_positive,
    create_dart_sql_server_conn,
)
from crawler.db.mongo import create_import_record, create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.db.mysql import create_mysql_connection, run_mysql_executemany_query
from crawler.filtered_positive_identifier import current_filtered_positive_identifier
from crawler.helpers.enums import CentreFileState
from crawler.helpers.general_helpers import (
    create_source_plate_doc,
    current_time,
    get_sftp_connection,
    map_mongo_sample_to_mysql,
)
from crawler.helpers.logging_helpers import LoggingCollection
from crawler.sql_queries import SQL_MLWH_MULTIPLE_INSERT
from crawler.types import CentreConf, CentreDoc, Config, CSVRow, ModifiedRow, RowSignature, SourcePlateDoc

logger = logging.getLogger(__name__)


PROJECT_ROOT = pathlib.Path(__file__).parent.parent
REGEX_FIELD = "sftp_file_regex"
ERRORS_DIR = "errors"
SUCCESSES_DIR = "successes"


class Centre:
    def __init__(self, config: Config, centre_config: CentreConf):
        self.config = config
        self.centre_config = centre_config
        self.is_download_dir_walkable = False

        # create backup directories for files
        os.makedirs(f"{self.centre_config['backups_folder']}/{ERRORS_DIR}", exist_ok=True)
        os.makedirs(f"{self.centre_config['backups_folder']}/{SUCCESSES_DIR}", exist_ok=True)

    def get_files_in_download_dir(self) -> List[str]:
        """Get all the files in the download directory for this centre and filter the file names using the regex
        described in the centre's 'regex_field'.

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

            self.is_download_dir_walkable = True

            return centre_files
        except Exception:
            self.is_download_dir_walkable = False

            logger.error(f"Failed when reading files from {path_to_walk}")
            return []

    def clean_up(self) -> None:
        """Remove the files downloaded from the SFTP for the given centre."""
        logger.debug(f"Remove files in {self.get_download_dir()}")
        try:
            shutil.rmtree(self.get_download_dir())

        except Exception as e:
            logger.error(f"Failed clean up: {e}")

    def process_files(self, add_to_dart: bool) -> None:
        """Iterate through all the files for the centre, parsing any new ones into the mongo database and then into the
        unified warehouse and optionally, DART.

        Arguments:
            add_to_dart {bool} -- whether to add the samples to DART
        """
        self.centre_files = sorted(self.get_files_in_download_dir())

        # iterate through each file in the centre
        for file_name in self.centre_files:
            logger.info(f"Checking file {file_name}")

            # create an instance of the file class to handle the file
            centre_file = CentreFile(file_name, self)

            centre_file.set_state_for_file()
            logger.debug(f"File state: {CentreFileState[centre_file.file_state.name]}")

            # Process depending on file state
            if centre_file.file_state == CentreFileState.FILE_IN_BLACKLIST:
                logger.info("File in blacklist, skipping")
                # next file
                continue
            elif centre_file.file_state == CentreFileState.FILE_NOT_PROCESSED_YET:
                # process it
                centre_file.process_samples(add_to_dart)
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
                logger.error(f"Unrecognised file state: {centre_file.file_state.name}")

    def get_download_dir(self) -> str:
        """Get the download directory where the files from the SFTP are stored.

        Returns:
            str -- the download directory
        """
        return f"{self.config.DIR_DOWNLOADED_DATA}{self.centre_config['prefix']}/"

    def download_csv_files(self) -> None:
        """Downloads the centre's file from the SFTP server"""
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


class CentreFile:
    """Class to process an individual file"""

    # we will be using re.IGNORECASE when making the match
    CHANNEL_REGEX_TEMPLATE: Final[str] = "^CH[ ]*{channel_number}[ ]*[-_][ ]*{word}$"

    # These headers are optional, and may not be present in all files from all lighthouses
    CHANNEL_FIELDS_MAPPING: Final[Dict[str, str]] = {
        FIELD_CH1_TARGET: CHANNEL_REGEX_TEMPLATE.format(channel_number=1, word="target"),
        FIELD_CH1_RESULT: CHANNEL_REGEX_TEMPLATE.format(channel_number=1, word="result"),
        FIELD_CH1_CQ: CHANNEL_REGEX_TEMPLATE.format(channel_number=1, word="cq"),
        FIELD_CH2_TARGET: CHANNEL_REGEX_TEMPLATE.format(channel_number=2, word="target"),
        FIELD_CH2_RESULT: CHANNEL_REGEX_TEMPLATE.format(channel_number=2, word="result"),
        FIELD_CH2_CQ: CHANNEL_REGEX_TEMPLATE.format(channel_number=2, word="cq"),
        FIELD_CH3_TARGET: CHANNEL_REGEX_TEMPLATE.format(channel_number=3, word="target"),
        FIELD_CH3_RESULT: CHANNEL_REGEX_TEMPLATE.format(channel_number=3, word="result"),
        FIELD_CH3_CQ: CHANNEL_REGEX_TEMPLATE.format(channel_number=3, word="cq"),
        FIELD_CH4_TARGET: CHANNEL_REGEX_TEMPLATE.format(channel_number=4, word="target"),
        FIELD_CH4_RESULT: CHANNEL_REGEX_TEMPLATE.format(channel_number=4, word="result"),
        FIELD_CH4_CQ: CHANNEL_REGEX_TEMPLATE.format(channel_number=4, word="cq"),
    }

    ACCEPTED_FIELDS = {
        FIELD_ROOT_SAMPLE_ID,
        FIELD_VIRAL_PREP_ID,
        FIELD_RNA_ID,
        FIELD_RNA_PCR_ID,
        FIELD_RESULT,
        FIELD_DATE_TESTED,
        FIELD_LAB_ID,
    }

    filtered_positive_identifier = current_filtered_positive_identifier()

    def __init__(self, file_name: str, centre: Centre):
        """Initialiser for the class representing the file

        Arguments:
            file_name {str} - the file name of the file
            centre {Dict[str][str]} -- the lighthouse centre
        """
        self.logging_collection = LoggingCollection()

        self.centre = centre
        self.config = centre.config
        self.centre_config = centre.centre_config
        self.file_name = file_name
        self.file_state = CentreFileState.FILE_UNCHECKED

        self.docs_inserted = 0

        # These headers are required in ALL files from ALL lighthouses
        self.required_fields = {
            FIELD_ROOT_SAMPLE_ID,
            FIELD_RNA_ID,
            FIELD_RESULT,
            FIELD_DATE_TESTED,
        }

        # These are to allow some variability in headers,
        # due to receiving inconsistent file formats
        self.header_regex_correction_dict = {r"^Root Sample$": FIELD_ROOT_SAMPLE_ID}

    def filepath(self) -> Path:
        """Returns the filepath for the file

        Returns:
            Path -- the filepath for the file
        """
        return PROJECT_ROOT.joinpath(f"{self.centre.get_download_dir()}{self.file_name}")

    def checksum(self) -> str:
        """Returns the checksum for the file

        Returns:
            str -- the checksum for the file
        """
        with open(self.filepath(), "rb") as file:
            file_hash = md5()
            while chunk := file.read(8192):
                file_hash.update(chunk)

        return file_hash.hexdigest()

    def checksum_match(self, dir_path: str) -> bool:
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
            if matches := regexp.match(backup_copy_file):
                # backup_timestamp = matches.group(1)
                backup_filename = matches.group(2)
                backup_checksum = matches.group(3)

                if checksum_for_file == backup_checksum:
                    if backup_filename != self.file_name:
                        logger.warning(
                            f"Found an identical file {backup_filename} in path {dir_path} which has the same checksum "
                            "but a different filename"
                        )
                    return True
        return False

    def get_centre_from_db(self) -> CentreDoc:
        """Gets a document from the mongo centre collection which describes a lighthouse centre.

        Raises:
            Exception: if no centre is found, raise an exception

        Returns:
            CentreDoc: mongo document describing a centre
        """
        centre_collection = get_mongo_collection(self.get_db(), COLLECTION_CENTRES)

        if centre := centre_collection.find_one({"name": self.centre_config["name"]}):
            return cast(CentreDoc, centre)

        raise Exception("Unable to find the centre in the centre collection.")

    def set_state_for_file(self) -> CentreFileState:
        """Determines what state the file is in and whether it needs to be processed.

        Returns:
            CentreFileState - enum representation of file state
        """
        centre = self.get_centre_from_db()

        # check whether file is on the blacklist and should be ignored
        if "file_names_to_ignore" in centre and self.file_name in centre["file_names_to_ignore"]:
            self.file_state = CentreFileState.FILE_IN_BLACKLIST

        # check whether file has already been processed to error directory
        elif self.checksum_match(ERRORS_DIR):
            self.file_state = CentreFileState.FILE_PROCESSED_WITH_ERROR

        # if checksum differs or file is not present in errors we check whether file has already been processed
        # successfully
        elif self.checksum_match(SUCCESSES_DIR):
            self.file_state = CentreFileState.FILE_PROCESSED_WITH_SUCCESS

        # if checksum(s) differs or if the file was not present in success directory, process it
        else:
            self.file_state = CentreFileState.FILE_NOT_PROCESSED_YET

        return self.file_state

    def process_samples(self, add_to_dart: bool) -> None:
        """Processes the samples extracted from the centre file.

        Arguments:
            add_to_dart {bool} -- whether to add the samples to DART
        """
        logger.info("Processing samples")

        # Internally traps TYPE 2: missing headers and TYPE 10 malformed files and returns
        docs_to_insert = self.process_csv()

        if self.logging_collection.get_count_of_all_errors_and_criticals() > 0:
            logger.error(f"Errors present in file {self.file_name}")
        else:
            logger.info(f"File {self.file_name} is valid")

        # Internally traps TYPE 26 failed assigning source plate UUIDs error and returns []
        docs_to_insert = self.docs_to_insert_updated_with_source_plate_uuids(docs_to_insert)

        if (num_docs_to_insert := len(docs_to_insert)) > 0:
            logger.debug(f"{num_docs_to_insert} docs to insert")

            mongo_ids_of_inserted = self.insert_samples_from_docs_into_mongo_db(docs_to_insert)

            if len(mongo_ids_of_inserted) > 0:
                # Filter out docs which failed to insert into mongo - we don't want to create MLWH records for these.
                docs_to_insert_mlwh = list(
                    filter(lambda x: x[FIELD_MONGODB_ID] in mongo_ids_of_inserted, docs_to_insert)
                )

                mlwh_success = self.insert_samples_from_docs_into_mlwh(docs_to_insert_mlwh)

                # add to the DART database if the config flag is set and we have successfully updated the MLWH
                if add_to_dart and mlwh_success:
                    logger.info("MLWH insert successful and adding to DART")

                    self.insert_plates_and_wells_from_docs_into_dart(docs_to_insert_mlwh)
        else:
            logger.info("No new docs to insert")

        self.backup_file()
        self.create_import_record_for_file()

    def backup_filename(self) -> str:
        """Backup the file.

        Returns:
            str -- the filepath of the file backup
        """
        if self.logging_collection.get_count_of_all_errors_and_criticals() > 0:
            return f"{self.centre_config['backups_folder']}/{ERRORS_DIR}/{self.timestamped_filename()}"
        else:
            return f"{self.centre_config['backups_folder']}/{SUCCESSES_DIR}/{self.timestamped_filename()}"

    def timestamped_filename(self) -> str:
        return f"{current_time()}_{self.file_name}_{self.checksum()}"

    def full_path_to_file(self) -> Path:
        return PROJECT_ROOT.joinpath(self.centre.get_download_dir(), self.file_name)

    def backup_file(self) -> None:
        """Backup the file."""
        destination = self.backup_filename()

        shutil.copyfile(self.full_path_to_file(), destination)

    def create_import_record_for_file(self) -> None:
        """Writes to the imports collection with information about the CSV file processed."""
        imports_collection = get_mongo_collection(self.get_db(), COLLECTION_IMPORTS)

        create_import_record(
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

    def add_duplication_errors(self, exception: BulkWriteError) -> None:
        """Add errors to the logging collection when we have the BulkWriteError exception.

        Args:
            exception (BulkWriteError): Exception with all the failed writes.
        """
        try:
            wrong_instances = [write_error["op"] for write_error in exception.details["writeErrors"]]
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
                        f"When trying to insert root_sample_id: "
                        f"{wrong_instance[FIELD_ROOT_SAMPLE_ID]}, contents: {wrong_instance}"
                    )
                    continue

                if entry[FIELD_DATE_TESTED] != wrong_instance[FIELD_DATE_TESTED]:
                    self.logging_collection.add_error(
                        "TYPE 7",
                        f"Already in database, line: {wrong_instance['line_number']}, root sample "
                        f"id: {wrong_instance['Root Sample ID']}, dates: "
                        f"({entry[FIELD_DATE_TESTED]} != {wrong_instance[FIELD_DATE_TESTED]})",
                    )
                else:
                    self.logging_collection.add_error(
                        "TYPE 6",
                        f"Already in database, line: {wrong_instance['line_number']}, root sample "
                        f"id: {wrong_instance['Root Sample ID']}",
                    )
        except Exception as e:
            logger.critical(f"Unknown error with file {self.file_name}: {e}")

    def docs_to_insert_updated_with_source_plate_uuids(self, docs_to_insert: List[ModifiedRow]) -> List[ModifiedRow]:
        """Updates sample records with source plate UUIDs, returning only those for which a source plate UUID could
        be determined. Adds any new source plates to mongo.

        Arguments:
            docs_to_insert {List[ModifiedRow]} -- the sample records to update

        Returns:
            List[Sample] -- the updated, filtered samples
        """
        logger.debug("Attempting to update docs with source plate UUIDs")

        updated_docs: List[ModifiedRow] = []

        def update_doc_from_source_plate(
            row: ModifiedRow, existing_plate: SourcePlateDoc, skip_lab_check: bool = False
        ) -> None:
            if skip_lab_check or row[FIELD_LAB_ID] == existing_plate[FIELD_LAB_ID]:
                row[FIELD_LH_SOURCE_PLATE_UUID] = existing_plate[FIELD_LH_SOURCE_PLATE_UUID]
                updated_docs.append(row)
            else:
                error_message = (
                    f"Source plate barcode {row[FIELD_PLATE_BARCODE]} in file {self.file_name} "
                    f"already exists with different lab_id {existing_plate[FIELD_LAB_ID]}",
                )
                self.logging_collection.add_error("TYPE 25", error_message)
                logger.error(error_message)

        try:
            new_plates: List[SourcePlateDoc] = []
            source_plates_collection = get_mongo_collection(self.get_db(), COLLECTION_SOURCE_PLATES)

            for doc in docs_to_insert:
                plate_barcode = doc[FIELD_PLATE_BARCODE]

                # first attempt an update from new plates (added for other samples in this file)
                existing_new_plate = next((x for x in new_plates if x[FIELD_BARCODE] == plate_barcode), None)
                if existing_new_plate is not None:
                    update_doc_from_source_plate(doc, existing_new_plate)
                    continue

                # then attempt an update from plates that exist in mongo
                existing_mongo_plate = source_plates_collection.find_one({FIELD_BARCODE: plate_barcode})
                if existing_mongo_plate is not None:
                    update_doc_from_source_plate(doc, existing_mongo_plate)
                    continue

                # then add a new plate
                new_plate = create_source_plate_doc(str(plate_barcode), str(doc[FIELD_LAB_ID]))
                new_plates.append(new_plate)
                update_doc_from_source_plate(doc, new_plate, True)

            if (new_plates_count := len(new_plates)) > 0:
                logger.debug(f"Attempting to insert {new_plates_count} new source plates")
                source_plates_collection.insert_many(new_plates, ordered=False)

        except Exception as e:
            self.logging_collection.add_error(
                "TYPE 26",
                f"Failed assigning source plate UUIDs to samples in file {self.file_name}",
            )
            logger.critical("Error assigning source plate UUIDs to samples in file " f"{self.file_name}: {e}")
            logger.exception(e)
            updated_docs = []

        return updated_docs

    def insert_samples_from_docs_into_mongo_db(self, docs_to_insert: List[ModifiedRow]) -> List[Any]:
        """Insert sample records into the mongo database from the parsed, filtered and modified CSV file information.

        Arguments:
            docs_to_insert {List[ModifiedRow]} -- list of filtered sample information extracted from CSV files
        """
        logger.debug(f"Attempting to insert {len(docs_to_insert)} docs")

        samples_collection = get_mongo_collection(self.get_db(), COLLECTION_SAMPLES)
        try:
            # Inserts new version for samples
            #  insert_many will add the '_id' field to each document inserted, making document["_id"] available
            # https://pymongo.readthedocs.io/en/stable/faq.html#writes-and-ids
            result = samples_collection.insert_many(docs_to_insert, ordered=False)

            self.docs_inserted = len(result.inserted_ids)

            # inserted_ids is in the same order as docs_to_insert, even if the query has ordered=False parameter
            return list(result.inserted_ids)

        # TODO could trap DuplicateKeyError specifically
        except BulkWriteError as e:
            # This is happening when there are duplicates in the data and the index prevents the records from being
            # written
            logger.warning("BulkWriteError: Usually happens when duplicates are trying to be inserted")

            # this can kill the crawler as the amount of duplicates logged can be huge
            # logger.debug(e)

            # filter out any errors that are duplicates by checking the code in e.details["writeErrors"]
            filtered_errors = list(filter(lambda x: x["code"] != 11000, e.details["writeErrors"]))

            if (num_filtered_errors := len(filtered_errors)) > 0:
                logger.info(
                    f"Number of exceptions left after filtering out duplicates = {num_filtered_errors}. Example:"
                )
                logger.info(filtered_errors[0])

            self.docs_inserted = e.details["nInserted"]

            self.add_duplication_errors(e)

            def get_errored_ids(error: Dict[str, Dict[str, str]]) -> str:
                """Get the object IDs from mongo of documents that failed to write.

                Arguments:
                    error (Dict[str, Dict[str, str]]): mongo error details

                Returns:
                    str: mongo object ID
                """
                return error["op"][FIELD_MONGODB_ID]

            errored_ids = list(map(get_errored_ids, e.details["writeErrors"]))

            logger.warning(f"{len(errored_ids)} records were not inserted")

            inserted_ids = [doc[FIELD_MONGODB_ID] for doc in docs_to_insert if doc[FIELD_MONGODB_ID] not in errored_ids]

            return inserted_ids
        except Exception as e:
            logger.critical(f"Critical error in file {self.file_name}: {e}")
            logger.exception(e)
            return []

    def insert_samples_from_docs_into_mlwh(self, docs_to_insert: List[ModifiedRow]) -> bool:
        """Insert sample records into the MLWH database from the parsed file information, including the corresponding
        mongodb _id

        Arguments:
            docs_to_insert {List[ModifiedRow]} -- List of filtered sample information extracted from CSV files.

        Returns:
            {bool} -- True if the insert was successful; otherwise False
        """
        values: List[Dict[str, Any]] = []

        for sample_doc in docs_to_insert:
            values.append(map_mongo_sample_to_mysql(sample_doc))

        mysql_conn = create_mysql_connection(self.config, False)

        if mysql_conn is not None and mysql_conn.is_connected():
            try:
                run_mysql_executemany_query(mysql_conn, SQL_MLWH_MULTIPLE_INSERT, values)

                logger.debug(f"MLWH database inserts completed successfully for file {self.file_name}")
                return True
            except Exception as e:
                self.logging_collection.add_error(
                    "TYPE 14",
                    f"MLWH database inserts failed for file {self.file_name}",
                )
                logger.critical(f"Critical error while processing file '{self.file_name}': {e}")
                logger.exception(e)
        else:
            self.logging_collection.add_error(
                "TYPE 15",
                f"MLWH database inserts failed, could not connect, for file {self.file_name}",
            )
            logger.critical(f"Error writing to MLWH for file {self.file_name}, could not create Database connection")

        return False

    def insert_plates_and_wells_from_docs_into_dart(self, docs_to_insert: List[ModifiedRow]) -> None:
        """Insert plates and wells into the DART database.

        Arguments:
            docs_to_insert {List[ModifiedRow]} -- List of filtered sample information extracted from CSV files.
        """
        if (sql_server_connection := create_dart_sql_server_conn(self.config)) is not None:
            try:
                cursor = sql_server_connection.cursor()

                for plate_barcode, samples in groupby_transform(  # type: ignore
                    docs_to_insert, lambda x: x[FIELD_PLATE_BARCODE]
                ):
                    try:
                        plate_state = add_dart_plate_if_doesnt_exist(
                            cursor, plate_barcode, self.centre_config["biomek_labware_class"]  # type: ignore
                        )
                        if plate_state == DART_STATE_PENDING:
                            for sample in samples:
                                add_dart_well_properties_if_positive(cursor, sample, plate_barcode)  # type: ignore
                        cursor.commit()
                    except Exception as e:
                        self.logging_collection.add_error(
                            "TYPE 22",
                            f"DART database inserts failed for plate {plate_barcode} in file {self.file_name}",
                        )
                        logger.exception(e)
                        # rollback statements executed since previous commit/rollback
                        cursor.rollback()

                logger.debug(f"DART database inserts completed successfully for file {self.file_name}")
            except Exception as e:
                self.logging_collection.add_error(
                    "TYPE 23",
                    f"DART database inserts failed for file {self.file_name}",
                )
                logger.critical(f"Critical error in file {self.file_name}: {e}")
                logger.exception(e)
            finally:
                sql_server_connection.close()
        else:
            self.logging_collection.add_error(
                "TYPE 24",
                f"DART database inserts failed, could not connect, for file {self.file_name}",
            )
            logger.critical(f"Error writing to DART for file {self.file_name}, could not create Database connection")

    def process_csv(self) -> List[ModifiedRow]:
        """Parses and processes the CSV file of the centre.

        Returns:
            List[ModifiedRow] -- the augmented data
        """
        csvfile_path = self.filepath()

        logger.info(f"Attempting to parse and process CSV file: {csvfile_path}")

        with open(csvfile_path, newline="") as csvfile:
            csvreader = DictReader(csvfile)

            try:
                self.remove_bom(csvreader)
                self.correct_headers(csvreader)

                # first check the required file headers are present
                if self.check_for_required_headers(csvreader):
                    # then parse and format the rows in the file
                    documents = self.parse_and_format_file_rows(csvreader)

                    return documents
            except (csv.Error, UnicodeDecodeError):
                self.logging_collection.add_error("TYPE 10", "Wrong read from file")

        return []

    def remove_bom(self, csvreader: DictReader) -> None:
        """Checks if there's a byte order mark (BOM) and removes it if so.
        We can't assume that the incoming file will or will not have one, have to cope with both.
        """
        if csvreader.fieldnames:
            first_fieldname = csvreader.fieldnames[0]

            as_bytes_from_utf8 = first_fieldname.encode("utf-8")
            has_bom = as_bytes_from_utf8[:3] == b"\xef\xbb\xbf"

            if has_bom:
                without_bom = as_bytes_from_utf8[3:].decode("utf-8")
                csvreader.fieldnames[0] = without_bom  # type: ignore

    def get_required_headers(self) -> Set[str]:
        """Returns the list of required headers.
        Includes Lab ID if config flag is set.

         Returns:
             {set} - the set of header names
        """
        if not self.config.ADD_LAB_ID:
            self.required_fields.add(FIELD_LAB_ID)

        return self.required_fields

    def get_channel_headers_mapping(self) -> Dict[str, str]:
        """Returns a dict of the channel fields and regex to match them.

        Returns:
            {Dict[str, str]} - mapping of channel field to regex
        """
        return self.CHANNEL_FIELDS_MAPPING

    def correct_headers(self, csvreader: DictReader) -> None:
        """Checks for any headers in the CSV file that are wrong but recognisable, and fixes them.
        Necessary due to variability in the file format we receive.
        """
        logger.debug("Checking CSV for nearly-correct header names and fixing them")

        if csvreader.fieldnames:
            for i, fieldname in enumerate(csvreader.fieldnames):
                for reg in self.header_regex_correction_dict.keys():
                    if re.match(reg, fieldname.strip()):
                        logger.warning(
                            f"Found '{reg}' in field name '{fieldname}', "
                            f"correcting to '{self.header_regex_correction_dict[reg]}'"
                        )
                        csvreader.fieldnames[i] = self.header_regex_correction_dict[reg]  # type: ignore

    def check_for_required_headers(self, csvreader: DictReader) -> bool:
        """Checks that the CSV file has the required headers.

        Raises:
            CentreFileError: Raised when the required fields are not found in the file
        """
        logger.debug("Checking CSV for required headers")

        if csvreader.fieldnames:
            fieldnames = set(csvreader.fieldnames)
            required = self.get_required_headers()

            if not required.issubset(fieldnames):
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

    def extract_plate_barcode_and_coordinate(
        self, row: Dict[str, Any], line_number: int, barcode_field: str, regex: str
    ) -> Tuple[str, str]:
        """Extracts fields from a row of data (from the CSV file). Currently extracting the barcode and coordinate (well
        position) using regex groups.

        We are using the re.ASCII flag so that \\w and \\W match ASCII values only.

        Arguments:
            row {Dict[str, Any]} -- row of data from CSV file
            barcode_field {str} -- field indicating the plate barcode of interest, might also include coordinate
            regex {str} -- regex pattern to use to extract the fields

        Returns:
            Tuple[str, str] -- the barcode and coordinate
        """
        pattern = re.compile(regex, re.ASCII)
        match = pattern.search(row[barcode_field].strip())
        # TODO: Update regex check to handle different format checks
        #  https://ssg-confluence.internal.sanger.ac.uk/pages/viewpage.action?pageId=101358138#ReceiptfromLighthouselaboratories(Largediagnosticcentres)-4.2.1VariantsofRNAplatebarcode

        if not match:
            sample_id = None
            if FIELD_ROOT_SAMPLE_ID in row:
                sample_id = row.get(FIELD_ROOT_SAMPLE_ID)

            self.logging_collection.add_error(
                "TYPE 9",
                f"Wrong reg. exp. {barcode_field}, line:{line_number}, "
                f"root_sample_id: {sample_id}, value: {row.get(barcode_field)}",
            )
            return "", ""

        return match.group(1), match.group(2)

    @staticmethod
    def create_row_signature(row: ModifiedRow) -> RowSignature:
        """Creates a "signature" for a row by returning a tuple of some fields in the row of data.

        Arguments:
            row (ModifiedRow): row object from csv.DictReader

        Returns:
            RowSignature: "signature" of row
        """
        signature: List[str] = []

        for field in (FIELD_ROOT_SAMPLE_ID, FIELD_RNA_ID, FIELD_RESULT, FIELD_LAB_ID):
            if field in row:
                signature.append(str(row[field]))

        return tuple(signature)

    def log_adding_default_lab_id(self, row, line_number):
        logger.debug(f"Adding in missing Lab ID for row {line_number}")
        self.logging_collection.add_error(
            "TYPE 12",
            f"No Lab ID, line: {line_number}, root_sample_id: {row.get(FIELD_ROOT_SAMPLE_ID)}",
        )

    def filtered_row(self, row: CSVRow, line_number: int) -> ModifiedRow:
        """Filter unneeded columns and add lab id if not present and config flag set.

        Arguments:
            row {CSVRow} - sample row read from file

        Returns:
            ModifiedRow - returns a modified version of the row
        """
        modified_row: ModifiedRow = {}

        if self.config.ADD_LAB_ID:
            # when we need to add the lab id if not present
            if (lab_id := row.get(FIELD_LAB_ID)) is not None:
                # if the lab id field is already present but it might be an empty string
                if not lab_id:
                    # if no value (empty string) we add the default value and log that it was missing
                    modified_row[FIELD_LAB_ID] = self.centre_config["lab_id_default"]

                    self.log_adding_default_lab_id(row, line_number)
                else:
                    if lab_id != self.centre_config["lab_id_default"]:
                        # if the lab id is different to what is configured for the lab
                        logger.warning(f"Different lab id setting: {lab_id} != {self.centre_config['lab_id_default']}")

                    # copy the lab id across
                    modified_row[FIELD_LAB_ID] = lab_id
            else:
                # if the lab id field is not present we add the default and log it was missing
                modified_row[FIELD_LAB_ID] = self.centre_config["lab_id_default"]
                self.log_adding_default_lab_id(row, line_number)

        seen_headers: List[str] = []

        # next check the row for values for each of the accepted fields (except for the CT fields) and copy them across
        for key in self.ACCEPTED_FIELDS:
            if key in row:
                seen_headers.append(key)
                modified_row[key] = row[key]

        # and check the row for values for any of the optional CT channel headers and copy them across
        seen_headers, modified_row = self.extract_channel_fields(seen_headers, row, modified_row)

        # now check if we still have any columns left in the file row that we do not recognise
        unexpected_headers = list(row.keys() - seen_headers)

        if len(unexpected_headers) > 0:
            self.logging_collection.add_error(
                "TYPE 13",
                f"Unexpected headers, line: {line_number}, "
                f"root_sample_id: {row.get(FIELD_ROOT_SAMPLE_ID)}, "
                f"extra headers: {unexpected_headers}",
            )

        return modified_row

    def extract_channel_fields(
        self, seen_headers: List[str], csv_row: CSVRow, modified_row: ModifiedRow
    ) -> Tuple[List[str], ModifiedRow]:
        """Extract the channel fields by trying to match the header fields using regex.

        Arguments:
            seen_headers (List[str]): headers already seen in the file
            csv_row (CSVRow): row from csv.DictReader
            modified_row (ModifiedRow): row with updated fields

        Returns:
            Tuple[List[str], ModifiedRow]: updated seen_headers and modified_row
        """
        for channel_field, regex in self.get_channel_headers_mapping().items():
            pattern = re.compile(regex, re.IGNORECASE)

            for csv_field in csv_row:
                if csv_field in seen_headers:
                    continue

                if pattern.match(csv_field):
                    seen_headers.append(csv_field)

                    if csv_row[csv_field]:
                        modified_row[channel_field] = csv_row[csv_field]

        return seen_headers, modified_row

    def parse_and_format_file_rows(self, csvreader: DictReader) -> List[ModifiedRow]:
        """Attempts to parse and format the file rows
           Adds additional derived and calculated fields to the imported rows that will aid querying
           later. Filters out blank rows, duplicated rows, and rows with values failing various
           rules on content. Creates error records for rows that do not pass checks, that will get
           written to the import logs for display in the Lighthouse-UI imports screen.

        Arguments:
            csvreader {DictReader} -- CSV file reader to iterate over

        Returns:
            List[ModifiedRow] -- list of errors and the augmented data
        """
        logger.debug("Adding extra fields")

        verified_rows: List[ModifiedRow] = []

        # Detect duplications and filters them out
        seen_rows: Set[RowSignature] = set()
        failed_validation_count = 0
        invalid_rows_count = 0
        line_number = 2

        for row in csvreader:
            # only process rows that have at least a minimum level of data
            if self.row_required_fields_present(row, line_number):
                if parsed_row := self.parse_and_format_row(row, line_number, seen_rows):
                    verified_rows.append(parsed_row)
                else:
                    # this counter catches rows where field validation failed
                    failed_validation_count += 1
            else:
                # this counter catches blank rows and rows with empty fields
                invalid_rows_count += 1

            line_number += 1

        logger.log(
            INFO if invalid_rows_count == 0 else WARN,
            f"Rows with invalid structure in this file = {invalid_rows_count}",
        )
        logger.log(
            INFO if failed_validation_count == 0 else WARN,
            f"Rows that failed validation in this file = {failed_validation_count}",
        )

        return verified_rows

    def parse_and_format_row(
        self, row: CSVRow, line_number: int, seen_rows: Set[RowSignature]
    ) -> Optional[ModifiedRow]:
        """Parses a single row and runs validations on content.

        Arguments:
            row {CSVRow} - row object from csv.DictReader
            line_number {int} - line number within the file
            seen_rows {tuple} - row signature of key values, used to exclude duplicates

        Returns:
            modified_row {Optional[ModifiedRow]} - modified filtered and formatted version of the row
        """
        # ---- create new row dict with just the recognized columns ----
        modified_row = self.filtered_row(row, line_number)

        # ---- check if this row has already been seen in this file, based on key fields ----
        row_signature = self.create_row_signature(modified_row)

        if row_signature in seen_rows:
            logger.debug(f"Skipping {row_signature}: duplicate")
            self.logging_collection.add_error(
                "TYPE 5",
                f"Duplicated, line: {line_number}, root_sample_id: {modified_row[FIELD_ROOT_SAMPLE_ID]}",
            )
            return None

        # ---- convert data types for channel fields ----
        if not self.convert_and_validate_cq_values(modified_row, line_number):
            return None

        # ---- perform various validations on row values ----
        # Check that the date is a valid format and if so, convert it to a datetime before saving to mongo
        date_format_valid, date_string_dict = self.is_valid_date_format(modified_row, line_number, FIELD_DATE_TESTED)
        if date_format_valid:
            if date_string_dict:
                # > By default all datetime.datetime objects returned by PyMongo will be naive but reflect UTC
                # https://pymongo.readthedocs.io/en/stable/examples/datetimes.html
                modified_row[FIELD_DATE_TESTED] = self.convert_datetime_string_to_datetime(**date_string_dict)
            else:
                modified_row[FIELD_DATE_TESTED] = None
        else:
            return None

        if not self.row_result_value_valid(modified_row, line_number):
            return None

        if not self.row_channel_target_values_valid(modified_row, line_number):
            return None

        if not self.row_channel_result_values_valid(modified_row, line_number):
            return None

        if not self.row_channel_cq_values_within_range(modified_row, line_number):
            return None

        if not self.row_positive_result_matches_channel_results(modified_row, line_number):
            return None

        # ---- add a few additional, computed or derived fields ----
        # add the centre name as source
        modified_row[FIELD_SOURCE] = self.centre_config["name"]

        # extract the barcode and well coordinate
        barcode_field = self.centre_config["barcode_field"]

        modified_row[FIELD_PLATE_BARCODE] = None
        if modified_row.get(barcode_field) and (barcode_regex := self.centre_config["barcode_regex"]):
            (
                modified_row[FIELD_PLATE_BARCODE],
                modified_row[FIELD_COORDINATE],
            ) = self.extract_plate_barcode_and_coordinate(modified_row, line_number, barcode_field, barcode_regex)

        if not modified_row.get(FIELD_PLATE_BARCODE):
            return None

        modified_row[FIELD_LINE_NUMBER] = line_number
        modified_row[FIELD_FILE_NAME] = self.file_name
        modified_row[FIELD_FILE_NAME_DATE] = self.file_name_date()
        modified_row[FIELD_CREATED_AT] = datetime.utcnow()
        modified_row[FIELD_UPDATED_AT] = datetime.utcnow()

        # filtered-positive calculations
        modified_row[FIELD_FILTERED_POSITIVE] = self.filtered_positive_identifier.is_positive(modified_row)
        modified_row[FIELD_FILTERED_POSITIVE_VERSION] = self.filtered_positive_identifier.version
        modified_row[FIELD_FILTERED_POSITIVE_TIMESTAMP] = datetime.utcnow()

        # add lh sample uuid
        modified_row[FIELD_LH_SAMPLE_UUID] = str(uuid.uuid4())

        # ---- store row signature to allow checking for duplicates in following rows ----
        seen_rows.add(row_signature)

        return modified_row

    def convert_and_validate_cq_values(self, row: ModifiedRow, line_number: int) -> bool:
        """Convert and validate each of the four channel fields.

        Arguments:
            row (ModifiedRow): modified filtered and formatted version of the row
            line_number (int): line number within the file

        Returns:
            bool: whether all channels were converted and passed validation
        """
        for channel_cq_field in (FIELD_CH1_CQ, FIELD_CH2_CQ, FIELD_CH3_CQ, FIELD_CH4_CQ):
            if not self.convert_and_validate_cq_value(row, channel_cq_field, line_number):
                return False

        return True

    def convert_and_validate_cq_value(self, row: ModifiedRow, channel_cq_field: str, line_number: int) -> bool:
        """Convert and validate a row's channel Cq field.

        Arguments:
            row (ModifiedRow): modified filtered and formatted version of the row
            channel_cq_field (str): the row's channel cq field to check
            line_number (int): line number within the file

        Returns:
            bool: whether the channel field was converted successfully
        """
        # check if this row has this channel field present, not all fields are expected so return True if not found
        if (channel_cq_field_val := row.get(channel_cq_field)) is None:
            return True
        elif channel_cq_field_val:
            try:
                # pymongo requires Decimal128 format for numbers rather than normal Decimal
                row[channel_cq_field] = Decimal128(channel_cq_field_val)
            except Exception:
                self.logging_collection.add_error(
                    "TYPE 19",
                    f"{channel_cq_field} invalid, line: {line_number}, value: {channel_cq_field_val}",
                )
                return False

        return True

    @staticmethod
    def convert_datetime_string_to_datetime(
        day: str, month: str, year: str, time: str, timezone_name: Optional[str] = None
    ) -> datetime:
        """Converts a datetime string (split in its components) into a python datetime

        Arguments:
            day (str): [description]
            month (str): Month as a zero-padded string
            year (str): Year with century
            time (str): hour, minute, and optionally seconds
            timezone_name (Optional[str], optional): Time zone name. Defaults to None.

        Returns:
            datetime: [description]
        """
        if len(time) == 5:
            time = f"{time}:00"

        datetime_string = f"{day} {month} {year} {time}"

        date_time = datetime.strptime(datetime_string, "%d %m %Y %H:%M:%S")

        # We are only checking for UTC at the moment, more time (excuse the pun) is needed to suppport timezones
        #   more robustly
        if timezone_name and timezone_name == "UTC":
            date_time = date_time.replace(tzinfo=timezone.utc)

        return date_time

    def is_valid_date_format(self, row: ModifiedRow, line_number: int, date_field: str) -> Tuple[bool, Dict[str, str]]:
        """The possible values for the date are:
        - '' (empty string)
        - YYYY-MM-DD HH:MM:SS Z e.g. 2020-11-22 04:36:38 UTC
        - DD/MM/YYYY HH:MM e.g. 19/07/2020 21:41

        Arguments:
            row (ModifiedRow): modified filtered and formatted version of the row
            line_number (int): line number within the file

        Returns:
            Tuple[bool, Dict[str, str]: whether the date format is valid and a dictionary of the date time components
        """
        # the date could be an empty string
        if not (date_field_val := row.get(date_field)):
            return True, {}
        else:
            for pattern in (
                r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})[ ]+(?P<time>[0-2]\d:[0-5]\d:[0-5]\d)([ ]+(?P<timezone_name>UTC)?)?$",  # noqa: E501
                r"^(?P<day>\d{2})/(?P<month>\d{2})/(?P<year>\d{4})[ ]+(?P<time>[0-2]\d:[0-5]\d)$",
            ):
                if match := re.match(pattern, str(date_field_val)):
                    return True, match.groupdict()

            # no patterns were matched to log an error
            self.logging_collection.add_error(
                "TYPE 27",
                f"{date_field} has an unknown date format, line: {line_number}",
            )
            return False, {}

    def row_result_value_valid(self, row: ModifiedRow, line_number: int) -> bool:
        """Validation to check if the row's 'Result' value is one of the expected values.

        Arguments:
            row {ModifiedRow} - modified filtered and formatted version of the row
            line_number {int} - line number within the file

        Returns:
            bool - whether the value is valid
        """
        if (result := row.get(FIELD_RESULT)) not in ALLOWED_RESULT_VALUES:
            self.logging_collection.add_error(
                "TYPE 16",
                f"{FIELD_RESULT} invalid, line: {line_number}, result: {result}",
            )
            return False

        return True

    def is_row_channel_target_valid(self, row: ModifiedRow, line_number: int, fieldname: str) -> bool:
        """Is the channel target valid.

        Arguments:
            row {ModifiedRow} - modified filtered and formatted version of the row
            line_number {int} - line number within the file
            fieldname {str} - the name of the target column

        Returns:
            bool - whether the value is valid
        """
        if (ch_target_value := row.get(fieldname)) is not None and ch_target_value not in ALLOWED_CH_TARGET_VALUES:
            self.logging_collection.add_error(
                "TYPE 17",
                f"{fieldname} invalid, line: {line_number}, result: {ch_target_value}",
            )
            return False

        return True

    def row_channel_target_values_valid(self, row: ModifiedRow, line_number: int) -> bool:
        """Validation to check if the row's channels' target values are one of the expected values.

        Arguments:
            row {ModifiedRow} - modified filtered and formatted version of the row
            line_number {integer} - line number within the file

        Returns:
            bool - whether all the channels' target values are correct
        """
        for channel_target_field in (FIELD_CH1_TARGET, FIELD_CH2_TARGET, FIELD_CH3_TARGET, FIELD_CH4_TARGET):
            if not self.is_row_channel_target_valid(row, line_number, channel_target_field):
                return False

        return True

    def is_row_channel_result_valid(self, row: ModifiedRow, line_number: int, channel_result_field: str) -> bool:
        """Is the channel result valid.

        Arguments:
            row {ModifiedRow} - modified filtered and formatted version of the row
            line_number {integer} - line number within the file
            channel_result_field {str} - the name of the result column

        Returns:
            bool - whether the result value is valid
        """
        if (
            channel_result_field_val := row.get(channel_result_field)
        ) is not None and channel_result_field_val not in ALLOWED_CH_RESULT_VALUES:
            self.logging_collection.add_error(
                "TYPE 18",
                f"{channel_result_field} invalid, line: {line_number}, result: {channel_result_field_val}",
            )
            return False

        return True

    def row_channel_result_values_valid(self, row: ModifiedRow, line_number: int) -> bool:
        """Validation to check if the row's channels' result values match one of the expected values.

        Arguments:
            row {ModifiedRow} - modified filtered and formatted version of the row
            line_number {int} - line number within the file

        Returns:
            bool - whether the values are valid
        """
        for channel_result_field in (FIELD_CH1_RESULT, FIELD_CH2_RESULT, FIELD_CH3_RESULT, FIELD_CH4_RESULT):
            if not self.is_row_channel_result_valid(row, line_number, channel_result_field):
                return False

        return True

    @staticmethod
    def is_within_cq_range(range_min: Decimal, range_max: Decimal, num: Decimal128) -> bool:
        """Validation to check if a number lies within the expected range.

        Arguments:
            range_min {Decimal} - minimum range number
            range_max {Decimal} - maximum range number
            num {Decimal128} - the number to be tested

        Returns:
            bool - whether the value lies within range
        """
        return range_min <= cast(Decimal, num.to_decimal()) <= range_max

    def is_row_channel_cq_in_range(self, row: ModifiedRow, line_number: int, fieldname: str) -> bool:
        """Is the channel cq within the specified range.

        Arguments:
            row {ModifiedRow} - modified filtered and formatted version of the row
            line_number {int} - line number within the file
            fieldname {str} - the name of the cq column

        Returns:
            bool - whether the cq value is valid
        """
        if (channel_cq_val := row.get(fieldname)) is not None and not self.is_within_cq_range(
            MIN_CQ_VALUE, MAX_CQ_VALUE, cast(Decimal128, channel_cq_val)
        ):
            self.logging_collection.add_error(
                "TYPE 20",
                f"{fieldname} not in range ({MIN_CQ_VALUE}, {MAX_CQ_VALUE}), "
                f"line: {line_number}, result: {channel_cq_val}",
            )
            return False

        return True

    def row_channel_cq_values_within_range(self, row: ModifiedRow, line_number: int) -> bool:
        """Validation to check if the row's channels' cq values are within range.

        Arguments:
            row {ModifiedRow} - modified filtered and formatted version of the row
            line_number {int} - line number within the file

        Returns:
            bool - whether the cq values are within range
        """
        for channel_cq_field in (FIELD_CH1_CQ, FIELD_CH2_CQ, FIELD_CH3_CQ, FIELD_CH4_CQ):
            if not self.is_row_channel_cq_in_range(row, line_number, channel_cq_field):
                return False

        return True

    def row_positive_result_matches_channel_results(self, row: ModifiedRow, line_number: int) -> bool:
        """Validation to check that when the result is positive, and channel results are present, then at least one of
        the channel results is also positive.

        Arguments:
            row {ModifiedRow} - modified filtered and formatted version of the row
            line_number {int} - line number within the file

        Returns:
            bool - whether the channel results complement the main results
        """
        # if the result is not positive we do not need to check any further
        if row.get(FIELD_RESULT) != POSITIVE_RESULT_VALUE:
            return True

        ch_results_present = 0
        ch_results_positive = 0

        # look for positive channel results
        for channel_result_field in (FIELD_CH1_RESULT, FIELD_CH2_RESULT, FIELD_CH3_RESULT, FIELD_CH4_RESULT):
            # check that a value is present and that it is not an empty string
            if (channel_result_val := row.get(channel_result_field)) is not None and channel_result_val:
                ch_results_present += 1
                if channel_result_val == POSITIVE_RESULT_VALUE:
                    ch_results_positive += 1

        # if there are no channel results present in the row we do not need to check further
        if ch_results_present == 0:
            return True

        # if there are no positives amongst the channel results, the row is invalid
        if ch_results_positive == 0:
            self.logging_collection.add_error(
                "TYPE 21",
                f"Positive Result does not match to CT Channel Results (none are positive), line: {line_number}",
            )
            return False

        return True

    def row_required_fields_present(self, row: CSVRow, line_number: int) -> bool:
        """Checks whether the row has the expected structure. Checks for:
        - blank rows
        - root sample id
        - result
        - barcode

        Arguments:
            row {CSVRow} - row object from the csvreader
            line_number {int} - line number within the file

        Returns:
            bool -- whether the row has valid structure or not
        """
        # check whether row is completely empty; this is OK but filter out row from further processing
        # all the values from the DictReader are strings, therefore we can do the following:
        #   bool("") is False, any("", "", "") will be False so we can know if a row is empty
        if not any(row.values()):
            self.logging_collection.add_error("TYPE 1", f"Empty line, line: {line_number}")

            return False

        # check that root sample id is present
        if not row.get(FIELD_ROOT_SAMPLE_ID):
            self.logging_collection.add_error("TYPE 3", f"Root Sample ID missing, line: {line_number}")

            logger.warning(f"We found line: {line_number} missing {FIELD_ROOT_SAMPLE_ID} but the line is not blank")

            return False

        # check that result is present
        if not row.get(FIELD_RESULT):
            self.logging_collection.add_error("TYPE 3", f"{FIELD_RESULT} missing, line: {line_number}")

            return False

        # check barcode is present
        barcode_field = self.centre_config["barcode_field"]
        if not row.get(barcode_field):
            self.logging_collection.add_error("TYPE 4", f"{barcode_field} missing, line: {line_number}")

            return False

        return True

    def file_name_date(self) -> Any:
        """Extracts date from the filename if it matches the expected format, otherwise returns None.

        Returns:
            datetime -- date and time extracted from the filename
        """
        # example filenames:
        # AP_sanger_report_200527_0818.csv
        # MK_sanger_report_200418_0800.csv
        # GLS_sanger_report_200529_2030.csv
        m = re.match(r".*_([\d]{6}_[\d]{4})\.csv", self.file_name)

        if not m:
            return None

        file_timestamp = m.group(1)

        return datetime.strptime(file_timestamp, "%y%m%d_%H%M")
