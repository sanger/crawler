import logging
import logging.config
import shutil
import os
from crawler.helpers import (current_time)

import pytest

from crawler.main import run

from crawler.constants import (
    COLLECTION_CENTRES,
    COLLECTION_IMPORTS,
    COLLECTION_SAMPLES,
)

from crawler.db import (
    get_mongo_collection,
)

# The run method encompasses the main actions of the crawler
# As a result, this may be considered an integration test,
# although it stops short of pulling the files in over FTP.
# Instead we disable the FTp by passing in false as the first argument
# and instead use a download directory that we populate prior to running the
# tests.


def test_run(mongo_database):
  _, mongo_database = mongo_database
  # Copy the test files to a new directory, as we expect run
  # to perform a clean up, and we don't want it cleaning up our
  # main copy of the data. We don't disable the clean up as:
  # 1) It also clears up the master files, which we'd otherwise need to handle
  # 2) It means we keep the tested process closer to the actual one
  _ = shutil.copytree("tests/files", "tmp/files", dirs_exist_ok=True)
  run(False, "crawler.config.integration")

  # We expect to have three collections following import
  centres_collection = get_mongo_collection(mongo_database, COLLECTION_CENTRES)
  imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
  samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)

  # We record our test centres
  assert centres_collection.count_documents({}) == 4
  assert centres_collection.count_documents({"name": "Test Centre"}) == 1

  # We record *all* our samples
  assert samples_collection.count_documents({}) == 12
  assert samples_collection.count_documents({"RNA ID": "123_B09", "source": "Alderley"}) == 1
  assert samples_collection.count_documents({"RNA ID": "123_H09", "source": "UK Biocentre"}) == 1

  # We get one import per centre
  assert imports_collection.count_documents({}) == 4

  # We clean up after ourselves
  (_, _, files) = next(os.walk("tmp/files"))
  assert 0 == len(files)


# If we have multiple runs, the older runs are archived with a timestamps
def test_repeat_run(mongo_database):
  _, mongo_database = mongo_database
  # Copy the test files to a new directory, as we expect run
  # to perform a clean up, and we don't want it cleaning up our
  # main copy of the data. We don't disable the clean up as:
  # 1) It also clears up the master files, which we'd otherwise need to handle
  # 2) It means we keep the tested process closer to the actual one
  _ = shutil.copytree("tests/files", "tmp/files", dirs_exist_ok=True)
  run(False, "crawler.config.integration")

  timestamp = current_time()

  _ = shutil.copytree("tests/files", "tmp/files", dirs_exist_ok=True)
  run(False, "crawler.config.integration", timestamp)
  # We expect to have three collections following import
  centres_collection = get_mongo_collection(mongo_database, COLLECTION_CENTRES)
  imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
  samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)

  previous_samples_collection = get_mongo_collection(mongo_database, f"{COLLECTION_SAMPLES}_{timestamp}")

  # We still have 4 test centers
  assert centres_collection.count_documents({}) == 4
  # We don't get extra samples
  assert samples_collection.count_documents({}) == 12
  # But we have the previous collection available
  assert previous_samples_collection.count_documents({}) == 12
  # We get additional imports
  assert imports_collection.count_documents({}) == 8

# If we have multiple runs, the older runs are archived with a timestamps


def test_error_run(mongo_database):
  _, mongo_database = mongo_database
  # Copy the test files to a new directory, as we expect run
  # to perform a clean up, and we don't want it cleaning up our
  # main copy of the data. We don't disable the clean up as:
  # 1) It also clears up the master files, which we'd otherwise need to handle
  # 2) It means we keep the tested process closer to the actual one
  _ = shutil.copytree("tests/files", "tmp/files", dirs_exist_ok=True)
  run(False, "crawler.config.integration")

  timestamp = current_time()

  _ = shutil.copytree("tests/files", "tmp/files", dirs_exist_ok=True)
  _ = shutil.copytree("tests/malformed_files", "tmp/files", dirs_exist_ok=True)

  run(False, "crawler.config.integration", timestamp)
  # We expect to have three collections following import
  centres_collection = get_mongo_collection(mongo_database, COLLECTION_CENTRES)
  imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
  samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)

  temporary_samples_collection = get_mongo_collection(
      mongo_database, f"tmp_{COLLECTION_SAMPLES}_{timestamp}")

  # We still have 4 test centers
  assert centres_collection.count_documents({}) == 4
  # The samples count should be the same as before
  assert samples_collection.count_documents({}) == 12

  # But we have the previous collection available at the point it errored
  assert temporary_samples_collection.count_documents({}) == 10
  # We get additional imports
  assert imports_collection.count_documents({}) == 8
