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
        FIELD_ROOT_SAMPLE_ID: 'MCM001',
        FIELD_CH1_CQ: Decimal128('20'),
        FIELD_CH2_CQ: Decimal128('24'),
        FIELD_CH3_CQ: Decimal128('30'),
    }

# ----- tests for current_version() -----

def test_current_version_is_latest():
    assert identifier.current_version() == identifier.versions[-1]

# ----- tests for is_positive() -----

def test_is_positive_returns_true_matching_criteria():
    # case invariant positive match
    sample = positive_sample()
    assert identifier.is_positive(sample) == True

    sample = positive_sample()
    sample[FIELD_RESULT] = 'POSITIVE'
    assert identifier.is_positive(sample) == True

    # 3x one of FIELD_CHX_CQ <= 30
    sample = positive_sample()
    sample[FIELD_CH2_CQ] = Decimal128('31')
    sample[FIELD_CH3_CQ] = Decimal128('31')
    assert identifier.is_positive(sample) == True

    sample = positive_sample()
    sample[FIELD_CH1_CQ] = Decimal128('31')
    sample[FIELD_CH3_CQ] = Decimal128('31')
    assert identifier.is_positive(sample) == True

    sample = positive_sample()
    sample[FIELD_CH1_CQ] = Decimal128('31')
    sample[FIELD_CH2_CQ] = Decimal128('31')
    assert identifier.is_positive(sample) == True

    # all FIELD_CHX_CQ None
    sample = positive_sample()
    sample[FIELD_CH1_CQ] = None
    sample[FIELD_CH2_CQ] = None
    sample[FIELD_CH3_CQ] = None
    assert identifier.is_positive(sample) == True

    # no FIELD_CHX_CQ fields
    sample = {
        FIELD_RESULT: POSITIVE_RESULT_VALUE,
        FIELD_ROOT_SAMPLE_ID: 'MCM001'
    }
    assert identifier.is_positive(sample) == True

def test_is_positive_returns_false_result_not_postive():
    sample = positive_sample()
    sample[FIELD_RESULT] = 'negative'
    assert identifier.is_positive(sample) == False

    sample = positive_sample()
    sample[FIELD_RESULT] = '  positive'
    assert identifier.is_positive(sample) == False

def test_is_positive_returns_false_control_sample():
    sample = positive_sample()
    sample[FIELD_ROOT_SAMPLE_ID] = 'CBIQA_MCM001'
    assert identifier.is_positive(sample) == False

def test_is_positive_returns_false_all_chx_cq_greater_than_30():
    sample = positive_sample()
    sample[FIELD_CH1_CQ] = Decimal128('31')
    sample[FIELD_CH2_CQ] = Decimal128('31')
    sample[FIELD_CH3_CQ] = Decimal128('33')
    assert identifier.is_positive(sample) == False


    