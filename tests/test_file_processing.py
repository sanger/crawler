import logging
import logging.config
import shutil
import os
from crawler.helpers import current_time

import pytest

from crawler.file_processing import process_files

from crawler.constants import (
    COLLECTION_CENTRES,
    COLLECTION_IMPORTS,
    COLLECTION_SAMPLES,
)

# NUMBER_CENTRES = 4
# NUMBER_VALID_SAMPLES = 12
# NUMBER_SAMPLES_ON_PARTIAL_IMPORT = 10

from crawler.db import get_mongo_collection

def test_process_files(mongo_database, config, testing_files_for_process):
    _, mongo_database = mongo_database
    logger = logging.getLogger(__name__)

    centre = config.CENTRES[0]
    centre["sftp_root_read"] = "tmp/files"
    errors: List[str] = []
    critical_errors: int = 0
    process_files(config, centre, logger, errors, critical_errors)

    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)

    # # We record *all* our samples
    assert samples_collection.count_documents({"RNA ID": "123_B09", "source": "Alderley"}) == 1

