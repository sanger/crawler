import pytest
from bson.decimal128 import Decimal128
from crawler.constants import (
    FIELD_CH1_CQ,
    FIELD_CH2_CQ,
    FIELD_CH3_CQ,
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    POSITIVE_RESULT_VALUE,
    LIMIT_OF_DETECTION_RESULT_VALUE,
)
from crawler.filtered_positive_identifier import (
    current_filtered_positive_identifier,
    filtered_positive_identifier_by_version,
    FILTERED_POSITIVE_VERSION_0,
    FILTERED_POSITIVE_VERSION_1,
    FILTERED_POSITIVE_VERSION_2,
    FilteredPositiveIdentifierV0,
    FilteredPositiveIdentifierV1,
    FilteredPositiveIdentifierV2,
)

# ----- test helpers -----


def positive_sample():
    return {
        FIELD_RESULT: POSITIVE_RESULT_VALUE,
        FIELD_ROOT_SAMPLE_ID: "MCM001",
        FIELD_CH1_CQ: Decimal128("5.12345678"),
        FIELD_CH2_CQ: Decimal128("6.12345678"),
        FIELD_CH3_CQ: Decimal128("7.12345678"),
    }


# ----- tests for current_filtered_positive_identifier() -----


def test_current_filtered_positive_identifier():
    identifier = current_filtered_positive_identifier()
    assert identifier.version == FILTERED_POSITIVE_VERSION_2


# ----- tests for filtered_positive_identifier_by_version() -----


def test_filtered_positive_identifier_by_version_returns_VO_identifier():
    identifier = filtered_positive_identifier_by_version(FILTERED_POSITIVE_VERSION_0)
    assert identifier.version == FILTERED_POSITIVE_VERSION_0


def test_filtered_positive_identifier_by_version_returns_V1_identifier():
    identifier = filtered_positive_identifier_by_version(FILTERED_POSITIVE_VERSION_1)
    assert identifier.version == FILTERED_POSITIVE_VERSION_1


def test_filtered_positive_identifier_by_version_returns_V2_identifier():
    identifier = filtered_positive_identifier_by_version(FILTERED_POSITIVE_VERSION_2)
    assert identifier.version == FILTERED_POSITIVE_VERSION_2


def test_filtered_positive_identifier_by_version_raises_unknown_version():
    with pytest.raises(ValueError):
        filtered_positive_identifier_by_version("unknown version")


# ----- tests for FilteredPositiveIdentifierV0 -----


def test_v0_version():
    identifier = FilteredPositiveIdentifierV0()
    assert identifier.version == FILTERED_POSITIVE_VERSION_0


def test_v0_is_positive_returns_true_matching_criteria():
    identifier = FilteredPositiveIdentifierV0()

    # expected positive match
    sample = positive_sample()
    assert identifier.is_positive(sample) is True

    # case invariant positive match
    sample = positive_sample()
    sample[FIELD_RESULT] = "POSITIVE"
    assert identifier.is_positive(sample) is True


def test_v0_is_positive_returns_false_result_not_positive():
    identifier = FilteredPositiveIdentifierV0()

    # does not conform to regex
    sample = positive_sample()
    sample[FIELD_RESULT] = "  positive"
    assert identifier.is_positive(sample) is False

    # negative result
    sample = positive_sample()
    sample[FIELD_RESULT] = "Negative"
    assert identifier.is_positive(sample) is False

    # void result
    sample = positive_sample()
    sample[FIELD_RESULT] = "Void"
    assert identifier.is_positive(sample) is False

    # 'limit of detection' result
    sample = positive_sample()
    sample[FIELD_RESULT] = LIMIT_OF_DETECTION_RESULT_VALUE
    assert identifier.is_positive(sample) is False


# ----- tests for FilteredPositiveIdentifierV1 -----


def test_v1_version():
    identifier = FilteredPositiveIdentifierV1()
    assert identifier.version == FILTERED_POSITIVE_VERSION_1


def test_v1_is_positive_returns_true_matching_criteria():
    identifier = FilteredPositiveIdentifierV1()

    # expected positive match
    sample = positive_sample()
    assert identifier.is_positive(sample) is True

    # case invariant positive match
    sample = positive_sample()
    sample[FIELD_RESULT] = "POSITIVE"
    assert identifier.is_positive(sample) is True

    # 3x mix of ct values
    sample = positive_sample()
    sample[FIELD_CH2_CQ] = Decimal128("41.12345678")
    sample[FIELD_CH3_CQ] = None
    assert identifier.is_positive(sample) is True

    sample = positive_sample()
    sample[FIELD_CH1_CQ] = None
    sample[FIELD_CH3_CQ] = Decimal128("42.12345678")
    assert identifier.is_positive(sample) is True

    sample = positive_sample()
    sample[FIELD_CH1_CQ] = Decimal128("40.12345678")
    sample[FIELD_CH2_CQ] = None
    assert identifier.is_positive(sample) is True

    # all ct values None
    sample = positive_sample()
    sample[FIELD_CH1_CQ] = None
    sample[FIELD_CH2_CQ] = None
    sample[FIELD_CH3_CQ] = None
    assert identifier.is_positive(sample) is True

    # no FIELD_CHX_CQ fields
    sample = {FIELD_RESULT: POSITIVE_RESULT_VALUE, FIELD_ROOT_SAMPLE_ID: "MCM001"}
    assert identifier.is_positive(sample) is True


def test_v1_is_positive_returns_false_result_not_positive():
    identifier = FilteredPositiveIdentifierV1()

    # does not conform to regex
    sample = positive_sample()
    sample[FIELD_RESULT] = "  positive"
    assert identifier.is_positive(sample) is False

    # negative result
    sample = positive_sample()
    sample[FIELD_RESULT] = "Negative"
    assert identifier.is_positive(sample) is False

    # void result
    sample = positive_sample()
    sample[FIELD_RESULT] = "Void"
    assert identifier.is_positive(sample) is False

    # 'limit of detection' result
    sample = positive_sample()
    sample[FIELD_RESULT] = LIMIT_OF_DETECTION_RESULT_VALUE
    assert identifier.is_positive(sample) is False


def test_v1_is_positive_returns_false_control_sample():
    identifier = FilteredPositiveIdentifierV1()

    sample = positive_sample()
    sample[FIELD_ROOT_SAMPLE_ID] = "CBIQA_MCM001"
    assert identifier.is_positive(sample) is False


def test_v1_is_positive_returns_false_all_ct_values_greater_than_30():
    identifier = FilteredPositiveIdentifierV1()

    sample = positive_sample()
    sample[FIELD_CH1_CQ] = Decimal128("40.12345678")
    sample[FIELD_CH2_CQ] = Decimal128("41.12345678")
    sample[FIELD_CH3_CQ] = Decimal128("42.12345678")
    assert identifier.is_positive(sample) is False


# ----- tests for FilteredPositiveIdentifierV2 -----


def test_v2_version():
    identifier = FilteredPositiveIdentifierV2()
    assert identifier.version == FILTERED_POSITIVE_VERSION_2


def test_v2_is_positive_returns_true_matching_criteria():
    identifier = FilteredPositiveIdentifierV2()

    # expected positive match
    sample = positive_sample()
    assert identifier.is_positive(sample) is True

    # case invariant positive match
    sample = positive_sample()
    sample[FIELD_RESULT] = "POSITIVE"
    assert identifier.is_positive(sample) is True

    # 3x mix of ct values
    sample = positive_sample()
    sample[FIELD_CH2_CQ] = Decimal128("41.12345678")
    sample[FIELD_CH3_CQ] = None
    assert identifier.is_positive(sample) is True

    sample = positive_sample()
    sample[FIELD_CH1_CQ] = None
    sample[FIELD_CH3_CQ] = Decimal128("42.12345678")
    assert identifier.is_positive(sample) is True

    sample = positive_sample()
    sample[FIELD_CH1_CQ] = Decimal128("40.12345678")
    sample[FIELD_CH2_CQ] = None
    assert identifier.is_positive(sample) is True

    # all ct values None
    sample = positive_sample()
    sample[FIELD_CH1_CQ] = None
    sample[FIELD_CH2_CQ] = None
    sample[FIELD_CH3_CQ] = None
    assert identifier.is_positive(sample) is True

    # no FIELD_CHX_CQ fields
    sample = {FIELD_RESULT: POSITIVE_RESULT_VALUE, FIELD_ROOT_SAMPLE_ID: "MCM001"}
    assert identifier.is_positive(sample) is True


def test_v2_is_positive_returns_false_result_not_positive():
    identifier = FilteredPositiveIdentifierV2()

    # does not conform to regex
    sample = positive_sample()
    sample[FIELD_RESULT] = "  positive"
    assert identifier.is_positive(sample) is False

    # negative result
    sample = positive_sample()
    sample[FIELD_RESULT] = "Negative"
    assert identifier.is_positive(sample) is False

    # void result
    sample = positive_sample()
    sample[FIELD_RESULT] = "Void"
    assert identifier.is_positive(sample) is False

    # 'limit of detection' result
    sample = positive_sample()
    sample[FIELD_RESULT] = LIMIT_OF_DETECTION_RESULT_VALUE
    assert identifier.is_positive(sample) is False

    # case invariant 'limit of detection' result
    sample = positive_sample()
    sample[FIELD_RESULT] = "LIMIT OF DETECTION"
    assert identifier.is_positive(sample) is False


def test_v2_is_positive_returns_false_control_sample():
    identifier = FilteredPositiveIdentifierV2()

    sample = positive_sample()
    sample[FIELD_ROOT_SAMPLE_ID] = "CBIQA_MCM001"
    assert identifier.is_positive(sample) is False

    sample = positive_sample()
    sample[FIELD_ROOT_SAMPLE_ID] = "QC0_MCM001"
    assert identifier.is_positive(sample) is False

    sample = positive_sample()
    sample[FIELD_ROOT_SAMPLE_ID] = "ZZA000_MCM001"
    assert identifier.is_positive(sample) is False


def test_v2_is_positive_returns_false_all_ct_values_greater_than_30():
    identifier = FilteredPositiveIdentifierV2()

    sample = positive_sample()
    sample[FIELD_CH1_CQ] = Decimal128("40.12345678")
    sample[FIELD_CH2_CQ] = Decimal128("41.12345678")
    sample[FIELD_CH3_CQ] = Decimal128("42.12345678")
    assert identifier.is_positive(sample) is False
