from crawler.filtered_positive_identifier import FilteredPositiveIdentifier
from bson.decimal128 import Decimal128
from crawler.constants import (
    POSITIVE_RESULT_VALUE,
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_CH1_CQ,
    FIELD_CH2_CQ,
    FIELD_CH3_CQ,
)

# ----- test helpers -----

identifier = FilteredPositiveIdentifier()


def positive_sample():
    return {
        FIELD_RESULT: POSITIVE_RESULT_VALUE,
        FIELD_ROOT_SAMPLE_ID: "MCM001",
        FIELD_CH1_CQ: Decimal128("5.12345678"),
        FIELD_CH2_CQ: Decimal128("6.12345678"),
        FIELD_CH3_CQ: Decimal128("7.12345678"),
    }


# ----- tests for current_version() -----


def test_current_version_is_latest():
    assert identifier.current_version() == identifier.versions[-1]


# ----- tests for is_positive() -----


def test_is_positive_returns_true_matching_criteria():
    # expected positive match
    sample = positive_sample()
    assert identifier.is_positive(sample) == True

    # case invariant positive match
    sample = positive_sample()
    sample[FIELD_RESULT] = "POSITIVE"
    assert identifier.is_positive(sample) == True

    # 3x mix of ct values
    sample = positive_sample()
    sample[FIELD_CH2_CQ] = Decimal128("41.12345678")
    sample[FIELD_CH3_CQ] = None
    assert identifier.is_positive(sample) == True

    sample = positive_sample()
    sample[FIELD_CH1_CQ] = None
    sample[FIELD_CH3_CQ] = Decimal128("42.12345678")
    assert identifier.is_positive(sample) == True

    sample = positive_sample()
    sample[FIELD_CH1_CQ] = Decimal128("40.12345678")
    sample[FIELD_CH2_CQ] = None
    assert identifier.is_positive(sample) == True

    # all ct values None
    sample = positive_sample()
    sample[FIELD_CH1_CQ] = None
    sample[FIELD_CH2_CQ] = None
    sample[FIELD_CH3_CQ] = None
    assert identifier.is_positive(sample) == True

    # no FIELD_CHX_CQ fields
    sample = {FIELD_RESULT: POSITIVE_RESULT_VALUE, FIELD_ROOT_SAMPLE_ID: "MCM001"}
    assert identifier.is_positive(sample) == True


def test_is_positive_returns_false_result_not_postive():
    # does not conform to regex
    sample = positive_sample()
    sample[FIELD_RESULT] = "  positive"
    assert identifier.is_positive(sample) == False

    # negative result
    sample = positive_sample()
    sample[FIELD_RESULT] = "Negative"
    assert identifier.is_positive(sample) == False

    # void result
    sample = positive_sample()
    sample[FIELD_RESULT] = "Void"
    assert identifier.is_positive(sample) == False

    # 'limit of detection' result
    sample = positive_sample()
    sample[FIELD_RESULT] = "limit of detection"
    assert identifier.is_positive(sample) == False


def test_is_positive_returns_false_control_sample():
    sample = positive_sample()
    sample[FIELD_ROOT_SAMPLE_ID] = "CBIQA_MCM001"
    assert identifier.is_positive(sample) == False


def test_is_positive_returns_false_all_ct_values_greater_than_30():
    sample = positive_sample()
    sample[FIELD_CH1_CQ] = Decimal128("40.12345678")
    sample[FIELD_CH2_CQ] = Decimal128("41.12345678")
    sample[FIELD_CH3_CQ] = Decimal128("42.12345678")
    assert identifier.is_positive(sample) == False
