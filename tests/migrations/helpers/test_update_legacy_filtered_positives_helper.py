import pytest

from migrations.helpers.update_legacy_filtered_positives_helper import (
    v0_version_set,
    unmigrated_mongo_samples,
)

# ----- migration helper function tests -----

def test_unmigrated_mongo_samples_returns_expected(config, filtered_positive_testing_samples):
    result = unmigrated_mongo_samples(config)
    assert len(result) == 1


def test_v0_version_set_returns_true_with_v0_samples(config, filtered_positive_testing_samples):
    assert v0_version_set(config) == True


def test_v0_version_set_returns_false_with_no_v0_samples(config, v1_filtered_positive_testing_samples):
    assert v0_version_set(config) == False


def test_v0_version_set_returns_false_with_no_version_fields(config, testing_samples):
    assert v0_version_set(config) == False


def test_get_v0_cherrypicked_samples_returns_expected(event_wh_data):
    
