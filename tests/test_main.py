import os
import shutil
from importlib import import_module, invalidate_caches
from unittest.mock import patch

from crawler.constants import (
    COLLECTION_CENTRES,
    COLLECTION_IMPORTS,
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    FIELD_CENTRE_NAME,
)
from crawler.db.mongo import get_mongo_collection
from crawler.main import run

NUMBER_CENTRES = 12
NUMBER_VALID_SAMPLES = 7
NUMBER_SAMPLES_ON_PARTIAL_IMPORT = 10
NUMBER_OF_FILES_PROCESSED = 10
NUMBER_ACCEPTED_SOURCE_PLATES = 4


# The run method encompasses the main actions of the crawler
# As a result, this may be considered an integration test,
# although it stops short of pulling the files in over FTP.
# Instead we disable the FTP by passing in false as the first argument
# and instead use a download directory that we populate prior to running the
# tests.


def test_run(mongo_database, baracoda, testing_files_for_process, pyodbc_conn):
    _, mongo_database = mongo_database
    with patch("crawler.file_processing.CentreFile.insert_samples_from_docs_into_mlwh"):
        run(False, False, False, "crawler.config.integration")

    # We expect to have four collections following import
    centres_collection = get_mongo_collection(mongo_database, COLLECTION_CENTRES)
    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)

    # We record our test centres
    assert centres_collection.count_documents({}) == NUMBER_CENTRES
    assert centres_collection.count_documents({FIELD_CENTRE_NAME: "Test Centre"}) == 1

    # We record all our source plates
    assert source_plates_collection.count_documents({}) == NUMBER_ACCEPTED_SOURCE_PLATES
    # Centres that we don't process unconsolidated files for
    assert source_plates_collection.count_documents({"barcode": "AP123"}) == 0
    assert source_plates_collection.count_documents({"barcode": "MK123"}) == 0
    assert source_plates_collection.count_documents({"barcode": "MK456"}) == 0
    assert source_plates_collection.count_documents({"barcode": "GLS123"}) == 0
    assert source_plates_collection.count_documents({"barcode": "GLS789"}) == 0
    # Centres that process all files
    assert source_plates_collection.count_documents({"barcode": "CB123"}) == 1
    assert source_plates_collection.count_documents({"barcode": "TS789"}) == 1

    # We record *all* our samples
    assert samples_collection.count_documents({}) == NUMBER_VALID_SAMPLES, (
        f"Wrong number of samples inserted. Expected: {NUMBER_VALID_SAMPLES}, Actual: "
        f"{samples_collection.count_documents({})}"
    )
    assert samples_collection.count_documents({"RNA ID": "CB123_A09", "source": "Cambridge-az"}) == 1
    assert samples_collection.count_documents({"RNA ID": "23JAN21-0001Q_A11", "source": "Randox"}) == 1

    # We get one import per centre
    assert imports_collection.count_documents({}) == NUMBER_OF_FILES_PROCESSED, (
        f"Wrong number of imports inserted. Expected: {NUMBER_OF_FILES_PROCESSED}, Actual: "
        f"{imports_collection.count_documents({})}"
    )

    # check number of success/error files for Alderley
    (_, _, files) = next(os.walk("tmp/backups/ALDP/successes"))
    assert len(files) == 0, f"Wrong number of success files. Expected: 0, Actual: {len(files)}"
    (_, _, files) = next(os.walk("tmp/backups/ALDP/errors"))
    assert len(files) == 3, f"Wrong number of error files. Expected: 3, Actual: {len(files)}"

    # check number of success/error files for Randox
    (_, _, files) = next(os.walk("tmp/backups/RAND/successes"))
    assert len(files) == 1, f"Wrong number of success files. Expected: 1, Actual: {len(files)}"
    (_, _, files) = next(os.walk("tmp/backups/RAND/errors"))
    assert len(files) == 0, f"Wrong number of error files. Expected: 0, Actual: {len(files)}"

    # check the code cleaned up the temporary files
    (_, subfolders, files) = next(os.walk("tmp/files/"))
    assert 0 == len(subfolders), f"Wrong number of subfolders. Expected: 0, Actual: {len(subfolders)}"


def test_error_run(mongo_database, baracoda, testing_files_for_process, pyodbc_conn):
    _, mongo_database = mongo_database

    with patch("crawler.file_processing.CentreFile.insert_samples_from_docs_into_mlwh"):
        run(False, False, False, "crawler.config.integration")

    # We expect to have four collections following import
    centres_collection = get_mongo_collection(mongo_database, COLLECTION_CENTRES)
    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    source_plates_collection = get_mongo_collection(mongo_database, COLLECTION_SOURCE_PLATES)

    # we expect files in the errors directory after the first run
    (_, _, files) = next(os.walk("tmp/backups/TEST/errors"))
    assert 2 == len(files)

    _ = shutil.copytree("tests/test_files/good", "tmp/files", dirs_exist_ok=True)
    _ = shutil.copytree("tests/test_files/malformed", "tmp/files", dirs_exist_ok=True)

    run(False, False, False, "crawler.config.integration")

    # The number of centres should be the same as before
    assert centres_collection.count_documents({}) == NUMBER_CENTRES
    # The source plates count should be the same as before
    assert source_plates_collection.count_documents({}) == NUMBER_ACCEPTED_SOURCE_PLATES
    # The samples count should be the same as before
    assert samples_collection.count_documents({}) == NUMBER_VALID_SAMPLES

    # We expect an additional file in the errors directory after the second run
    (_, _, files) = next(os.walk("tmp/backups/TEST/errors"))
    assert 3 == len(files)

    # We get an additional imports
    assert imports_collection.count_documents({}) == NUMBER_OF_FILES_PROCESSED + 1


def test_error_run_duplicates_in_imports_message(mongo_database, baracoda, testing_files_for_process, pyodbc_conn):
    _, mongo_database = mongo_database

    # copy an additional file with duplicates
    _ = shutil.copytree("tests/test_files/duplicate_samples", "tmp/files", dirs_exist_ok=True)

    with patch("crawler.file_processing.CentreFile.insert_samples_from_docs_into_mlwh"):
        run(False, False, False, "crawler.config.integration")

    # Fetch the imports collection, expect it to contain the additional duplicate error file record
    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    assert imports_collection.count_documents({}) == NUMBER_OF_FILES_PROCESSED + 1

    # Fetch the Test centre record
    test_centre_imports = imports_collection.find_one({"centre_name": "Test Centre"})

    assert test_centre_imports is not None

    # We expect 2 errors for this file, type 5 (duplicates) errors, 1 message and 1 aggregate count
    assert len(test_centre_imports["errors"]) == 2

    # We expect errors to contain messages for type 5 duplicates, an aggregate total and a message
    # line
    assert "Total number of 'Duplicates within file' errors (TYPE 5): 1" in test_centre_imports["errors"][0]
    assert (
        "WARNING: Duplicates detected within the file. (TYPE 5) (e.g. Duplicated, line: 3, root_sample_id: 16)"
    ) in test_centre_imports["errors"][1]


def test_error_run_duplicates_plate_barcodes_from_different_labs_message(
    mongo_database, baracoda, testing_files_for_process, pyodbc_conn
):
    _, mongo_database = mongo_database

    # copy an additional file with duplicates
    _ = shutil.copytree("tests/test_files/duplicate_barcodes", "tmp/files", dirs_exist_ok=True)

    with patch("crawler.file_processing.CentreFile.insert_samples_from_docs_into_mlwh"):
        run(False, False, False, "crawler.config.integration")

    # Fetch the imports collection, expect it to contain the additional duplicate error file record
    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    assert imports_collection.count_documents({}) == NUMBER_OF_FILES_PROCESSED + 1

    # Fetch the Test centre record
    test_centre_imports = imports_collection.find_one({"centre_name": "Test Centre"})

    assert test_centre_imports is not None

    # We expect 2 errors for this file, type 5 (duplicates) errors, 1 message and 1 aggregate count
    assert len(test_centre_imports["errors"]) == 2

    # We expect errors to contain messages for type 24 duplicates, an aggregate total and a message
    # line
    assert (
        "Total number of 'Duplicate source plate barcodes from different labs' errors (TYPE 25): 2"
        in test_centre_imports["errors"][0]
    )
    assert ("ERROR: Found duplicate source plate barcodes from different labs (TYPE 25)") in test_centre_imports[
        "errors"
    ][1]


def test_run_creates_right_files_backups(mongo_database, baracoda, testing_files_for_process, pyodbc_conn):
    """
    NBNBNB!!!

    This test causes problems with ignoring files when run BEFORE other tests in this file. It is to do with the
    config file (crawler.config.integration_with_blacklist_change) writing over the centre config for the
    subsequent tests.

    I was not able to get to the bottom of it... :(

    """
    _, mongo_database = mongo_database
    # First copy the test files to a new directory, as we expect run
    # to perform a clean up, and we don't want it cleaning up our
    # main copy of the data. We don't disable the clean up as:
    # 1) It also clears up any modified test files, which we'd otherwise need to handle
    # 2) It means we keep the tested process closer to the actual one
    with patch("crawler.file_processing.CentreFile.insert_samples_from_docs_into_mlwh"):
        run(False, False, False, "crawler.config.integration")

    # check number of success/error files after first run
    (_, _, files) = next(os.walk("tmp/backups/ALDP/successes"))
    assert 0 == len(files)

    (_, _, files) = next(os.walk("tmp/backups/ALDP/errors"))
    assert 3 == len(files)

    (_, _, files) = next(os.walk("tmp/backups/CAMC/successes"))
    assert 1 == len(files), "Fail success CAMC"

    (_, _, files) = next(os.walk("tmp/backups/CAMC/errors"))
    assert 0 == len(files)

    (_, _, files) = next(os.walk("tmp/backups/MILK/successes"))
    assert 0 == len(files)

    (_, _, files) = next(os.walk("tmp/backups/MILK/errors"))
    assert 2 == len(files)

    (_, _, files) = next(os.walk("tmp/backups/TEST/successes"))
    assert 1 == len(files), "Fail success TEST"

    (_, _, files) = next(os.walk("tmp/backups/TEST/errors"))
    assert 2 == len(files)

    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    assert imports_collection.count_documents({}) == NUMBER_OF_FILES_PROCESSED

    # Second run to test that already processed files are skipped
    # and that a file previously in the blacklist is now processed
    # First copy full set of files as before.
    _ = shutil.copytree("tests/test_files/good", "tmp/files", dirs_exist_ok=True)

    # Invalidate old copy of config
    invalidate_caches()

    try:
        # Delete the mongo centres collection so that it gets repopulated from the new config this run
        centres_collection = get_mongo_collection(mongo_database, COLLECTION_CENTRES)
        centres_collection.drop()

        # Run with a different config that does not blacklist one of the files
        with patch("crawler.file_processing.CentreFile.insert_samples_from_docs_into_mlwh"):
            run(False, False, False, "crawler.config.integration_with_blacklist_change")

        # We expect an additional import entry
        assert imports_collection.count_documents({}) == NUMBER_OF_FILES_PROCESSED + 1

        # We expect the previously blacklisted file to now be processed
        (_, _, files) = next(os.walk("tmp/backups/TEST/successes"))
        assert 2 == len(files), (
            f"Wrong number of success files. Expected: 2, actual: {len(files)}. Previously "
            "blacklisted file should have been processed."
        )

        # We expect the previous blacklisted file to still be in the errors directory as well
        (_, _, files) = next(os.walk("tmp/backups/TEST/errors"))
        assert 2 == len(files)

        # check the code cleaned up the temporary files
        (_, subfolders, files) = next(os.walk("tmp/files/"))
        assert 0 == len(subfolders)
    finally:
        invalidate_caches()
        import_module("crawler.config.integration")
