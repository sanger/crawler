from unittest.mock import patch

import pytest

from migrations import update_legacy_filtered_positives

# ----- test migration -----


def test_exception_raised_if_v0_filtered_positives_present_in_mongo(filtered_positive_testing_samples):
    with pytest.raises(Exception):
        # call the migration
        update_legacy_filtered_positives.run("crawler.config.integration")


def test_no_unmigrated_samples_found_raises_exception():
    with pytest.raises(Exception):
        with patch(
            "migrations.helpers.update_legacy_filtered_positives_helper.unmigrated_mongo_samples", return_value=[]
        ):
            update_legacy_filtered_positives.run("crawler.config.integration")
