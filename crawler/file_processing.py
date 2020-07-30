from crawler.constants import (
    COLLECTION_SAMPLES,
)
from crawler.helpers import (
    get_files_in_download_dir,
    parse_csv
)
from crawler.db import (
    get_mongo_collection,
    get_mongo_db,
    create_mongo_client
)
from pymongo.errors import BulkWriteError

def process_files(config, centre, logger, errors, critical_errors) -> None:
    """Iterate through all the files for the centre, parsing any new ones into
    the database.

    Arguments:
        config {ModuleType} -- app config
        centre {Dict[str, str]} -- the centre in question
    """
    logger.info(f"Fetching files of {centre['name']}")

    # get list of files
    centre_files = get_files_in_download_dir(config, centre, "sftp_file_regex")

    # iterate each file in the centre
    for file_name in sorted(centre_files):
        logger.info(f"Checking file {file_name}")

        file_condition = get_state_for_file(file_name)
        # get_file_status()
        #
        # When processing the file:
        # FILE_IN_BLACKLIST
        # FILE_NOT_PROCESSED_YET -> checksum and filename not in any of the folders
        # FILE_PROCESSED_WITH_ERROR
        # FILE_PROCESSED_WITH_SUCCESS

        # check_and_filter_released(new_samples)
        # archive_old_samples(new_samples)
        # insert_new_samples(new_samples)


        #
        # If FILE_NOT_PROCESSED_YET
        #  create sample
        # If FILE_PROCESSED
        #
        # check whether file is on the blacklist and should be ignored
        if file_name in centre["file_names_to_ignore"]:
            continue

        # check whether file has already been processed
        # if file_name in centre["file_names_processed"]:
            # TODO: need to compare file details to prev version in BOTH errors and processed files volume
            # if the same, continue, if different, re-process
            # continue


        logger.info(f"Processing file {file_name}")
        parse_errors, docs_to_insert = parse_csv(config, centre, file_name)
        if parse_errors:
            logger.info(f"Errors present in file {file_name}")
            # error handling - log error
            # write to volume 'errors'
            # add filename to blacklist?
            continue
        else:
            logger.info(f"File valid")
            # TODO: if this is a new version of a file we already processed do we overwrite it?
            # or rename the previous one with a timestamp?
            # write to volume 'processed'

        insert_samples_from_docs(config, docs_to_insert, logger, errors, critical_errors)

def insert_samples_from_docs(config, docs_to_insert, logger, errors, critical_errors) -> None:
    """Insert sample records from the parsed file information.

    Arguments:
        docs_to_insert {List[Dict[str, str]]} -- list of sample information extracted from csv files
    """
    logger.debug(f"Attempting to insert {len(docs_to_insert)} docs")
    docs_inserted = 0
    client = create_mongo_client(config)
    db = get_mongo_db(config, client)
    samples_collection = get_mongo_collection(db, COLLECTION_SAMPLES)
    try:
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
            errors.append(f"{num_errors} records with error code {error}")
    except Exception as e:
        errors.append(f"Critical error: {e}")
        critical_errors += 1
        logger.exception(e)