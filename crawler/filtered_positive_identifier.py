from abc import ABC
import decimal
import re

from bson.decimal128 import create_decimal128_context  # type: ignore

from crawler.constants import (
    FIELD_CH1_CQ,
    FIELD_CH2_CQ,
    FIELD_CH3_CQ,
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    POSITIVE_RESULT_VALUE,
    LIMIT_OF_DETECTION_RESULT_VALUE,
)
from crawler.types import Sample


# record/reference all versions and definitions here
FILTERED_POSITIVE_VERSION_0 = "v0"  # pre-filtered_positive definitions
FILTERED_POSITIVE_VERSION_1 = "v1"  # initial implementation, as per GPL-669
FILTERED_POSITIVE_VERSION_2 = "v2"  # updated as per GPL-699 and GPL-740


class FilteredPositiveIdentifier(ABC):
    def __init__(self):
        self.version = None
        self.ct_value_limit = decimal.Decimal(30)
        self.d128_context = create_decimal128_context()
        self.result_regex = None
        self.root_sample_id_control_regex = None
        self.evaluate_ct_values = False

    def is_positive(self, sample: Sample) -> bool:
        """Determines whether a sample is a filtered positive.

        Arguments:
            sample {Sample} -- information on a single sample

        Returns:
            {bool} -- whether the sample is a filtered positive
        """
        if self.result_regex.match(sample[FIELD_RESULT]) is None:
            return False

        if (
            self.root_sample_id_control_regex
            and self.root_sample_id_control_regex.match(sample[FIELD_ROOT_SAMPLE_ID]) is not None
        ):
            return False

        if self.evaluate_ct_values:
            ch1_cq = sample.get(FIELD_CH1_CQ)
            ch2_cq = sample.get(FIELD_CH2_CQ)
            ch3_cq = sample.get(FIELD_CH3_CQ)

            if ch1_cq is None and ch2_cq is None and ch3_cq is None:
                return True

            with decimal.localcontext(self.d128_context):
                # type check before attempting to convert to decimal
                if ch1_cq is not None and ch1_cq.to_decimal() <= self.ct_value_limit:
                    return True
                elif ch2_cq is not None and ch2_cq.to_decimal() <= self.ct_value_limit:
                    return True
                elif ch3_cq is not None and ch3_cq.to_decimal() <= self.ct_value_limit:
                    return True
                else:
                    return False
        else:
            return True


def current_filtered_positive_identifier() -> FilteredPositiveIdentifier:
    return FilteredPositiveIdentifierV2()


class FilteredPositiveIdentifierV0(FilteredPositiveIdentifier):
    def __init__(self):
        super(FilteredPositiveIdentifierV0, self).__init__()
        self.version = FILTERED_POSITIVE_VERSION_0
        self.result_regex = re.compile(f"^{POSITIVE_RESULT_VALUE}", re.IGNORECASE)


class FilteredPositiveIdentifierV1(FilteredPositiveIdentifier):
    def __init__(self):
        super(FilteredPositiveIdentifierV1, self).__init__()
        self.version = FILTERED_POSITIVE_VERSION_1
        self.result_regex = re.compile(f"^{POSITIVE_RESULT_VALUE}", re.IGNORECASE)
        self.root_sample_id_control_regex = re.compile("^CBIQA_")
        self.evaluate_ct_values = True


class FilteredPositiveIdentifierV2(FilteredPositiveIdentifier):
    def __init__(self):
        super(FilteredPositiveIdentifierV2, self).__init__()
        self.version = FILTERED_POSITIVE_VERSION_2
        self.result_regex = re.compile(f"^(?:{POSITIVE_RESULT_VALUE}|{LIMIT_OF_DETECTION_RESULT_VALUE})", re.IGNORECASE)
        self.root_sample_id_control_regex = re.compile("^(?:CBIQA_|QC0|ZZA000)")
        self.evaluate_ct_values = True
