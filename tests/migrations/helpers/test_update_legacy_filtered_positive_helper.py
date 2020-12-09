import pytest

from migrations.helpers.update_legacy_filtered_positive_helper import (
    all_mongo_samples,
)

# ----- test fixture helpers -----
# ----- migration helper function tests -----


def test_all_mongo_samples_returns_expected(config, testing_samples):
    expected_samples = testing_samples  # only the first sample is positive, with matching plate barcode
    result = all_mongo_samples(config)
    assert result == expected_samples
