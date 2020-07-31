import logging
import logging.config
import shutil
import os
from crawler.helpers import current_time
from unittest.mock import patch
import pytest

from crawler.file_processing import (
    Centre,
    CentreFile
)
from crawler.constants import (
    COLLECTION_CENTRES,
    COLLECTION_IMPORTS,
    COLLECTION_SAMPLES,
    COLLECTION_SAMPLES_HISTORY,
    FIELD_ROOT_SAMPLE_ID,
)

from crawler.db import get_mongo_collection

def test_archival_prepared_sample_conversor_changes_data(config):
    timestamp = '20/12/20'

    centre = Centre(config, config.CENTRES[0])

    with patch(
        "crawler.file_processing.current_time", return_value=timestamp,
    ):
        centre_file = CentreFile('some file', centre)

        val = centre_file.archival_prepared_sample_conversor({'_id': '1234', 'name': '4567'}, timestamp)
        assert val == {'sample_object_id': '1234', 'archived_at': '20/12/20', 'name': '4567'}


def test_archival_prepared_samples_adds_timestamp(config):
    timestamp = '20/12/20'

    centre = Centre(config, config.CENTRES[0])

    with patch(
        "crawler.file_processing.current_time", return_value=timestamp,
    ):
        centre_file = CentreFile('some file', centre)

        val = centre_file.archival_prepared_samples([{'_id': '1234', 'name': '4567'}, {'_id': '4567', 'name': '1234'}])
        assert val == [
            {'sample_object_id': '1234', 'archived_at': '20/12/20', 'name': '4567'},
            {'sample_object_id': '4567', 'archived_at': '20/12/20', 'name': '1234'}
        ]


def test_archive_old_samples(config, testing_samples, samples_history_collection_accessor, samples_collection_accessor):
    sample_object_ids = list(map(lambda x: x['_id'], testing_samples))
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile('some file', centre)
    current_archived_samples = samples_history_collection_accessor.find({"sample_object_id": {"$in": sample_object_ids}})
    assert current_archived_samples.count() == 0
    centre_file.archive_old_samples(testing_samples)

    archived_samples = samples_history_collection_accessor.find({"sample_object_id": {"$in": sample_object_ids}})
    assert len(testing_samples) == archived_samples.count()
    assert samples_collection_accessor.find({"Root Sample ID": {"$in": sample_object_ids}}).count() == 0

def test_archive_old_samples_without_previous_samples(config, samples_history_collection_accessor, samples_collection_accessor):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile('some file', centre)

    assert samples_collection_accessor.count() == 0
    assert samples_history_collection_accessor.count() == 0

    centre_file.archive_old_samples([{"Root Sample ID": "1"}])

    assert samples_history_collection_accessor.count() == 0
    assert samples_collection_accessor.count() == 0

def test_get_download_dir(config):
    centre = Centre(config, config.CENTRES[0])

    assert centre.get_download_dir() == 'tests/files/ALDP/'

def test_get_filename_with_checksum():
    return True

def test_set_state_for_file_when_file_in_black_list():
    return False

def test_set_state_for_file_when_never_seen_before():
    return False

def test_set_state_for_file_when_in_error_folder():
    return False

def test_set_state_for_file_when_in_success_folder():
    return False

def test_process_files(mongo_database, config, testing_files_for_process):
    _, mongo_database = mongo_database
    logger = logging.getLogger(__name__)

    centre_config = config.CENTRES[0]
    centre_config["sftp_root_read"] = "tmp/files"
    centre = Centre(config, centre_config)
    centre.process_files()

    imports_collection = get_mongo_collection(mongo_database, COLLECTION_IMPORTS)
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    samples_history_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES_HISTORY)

    # # We record *all* our samples
    assert samples_collection.count_documents({"RNA ID": "123_B09", "source": "Alderley"}) == 1

