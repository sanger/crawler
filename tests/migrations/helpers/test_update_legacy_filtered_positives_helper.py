import pytest

from migrations.helpers.update_legacy_filtered_positives_helper import (
    all_mongo_samples,
    v0_version_set,
)

# ----- migration helper function tests -----


def test_all_mongo_samples_returns_expected(config, testing_samples):
    expected_samples = testing_samples  # only the first sample is positive, with matching plate barcode
    result = all_mongo_samples(config)
    assert result == expected_samples


def test_v0_version_set_returns_true_with_v0_samples(config, filtered_positive_testing_samples):
    assert v0_version_set(config) == True


def test_v0_version_set_returns_false_with_no_v0_samples(config, v1_filtered_positive_testing_samples):
    assert v0_version_set(config) == False


def test_v0_version_set_returns_true_with_no_version_fields(config, testing_samples):
    assert v0_version_set(config) == False
