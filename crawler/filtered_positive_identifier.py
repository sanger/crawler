import decimal
import re
from abc import ABC
from typing import Optional, Pattern, cast

from bson.decimal128 import Decimal128, create_decimal128_context

from crawler.constants import (
    FIELD_CH1_CQ,
    FIELD_CH2_CQ,
    FIELD_CH3_CQ,
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    RESULT_VALUE_POSITIVE,
)
from crawler.types import SampleDoc

# record/reference all versions and definitions here
FILTERED_POSITIVE_VERSION_0 = "v0"  # pre-filtered_positive definitions
FILTERED_POSITIVE_VERSION_1 = "v1"  # initial implementation, as per GPL-669
FILTERED_POSITIVE_VERSION_2 = "v2"  # updated as per GPL-699 and GPL-740


class FilteredPositiveIdentifier(ABC):
    def __init__(self):
        self.version: str = ""
        self.ct_value_limit = decimal.Decimal(30)
        self.d128_context = create_decimal128_context()
        self.result_regex = re.compile(f"^{RESULT_VALUE_POSITIVE}", re.IGNORECASE)
        self.root_sample_id_control_regex: Optional[Pattern[str]] = None
        self.evaluate_ct_values = False

    def is_positive(self, sample: SampleDoc) -> bool:
        """Determines whether a sample is a filtered positive.

        Arguments:
            sample {Sample} -- information on a single sample

        Returns:
            {bool} -- whether the sample is a filtered positive
        """
        if not self.result_regex.match(str(sample[FIELD_RESULT])):
            return False

        if self.root_sample_id_control_regex and self.root_sample_id_control_regex.match(
            str(sample[FIELD_ROOT_SAMPLE_ID])
        ):
            return False

        if self.evaluate_ct_values:
            ch1_cq = sample.get(FIELD_CH1_CQ)
            ch2_cq = sample.get(FIELD_CH2_CQ)
            ch3_cq = sample.get(FIELD_CH3_CQ)

            if ch1_cq is None and ch2_cq is None and ch3_cq is None:
                return True

            #  Since we are dealing with dictionary objects whose values could be of any type, we need to cast here to
            #   keep typing happy before we do a refactor to proper objects for samples
            ch1_cq = cast(Decimal128, ch1_cq)
            ch2_cq = cast(Decimal128, ch2_cq)
            ch3_cq = cast(Decimal128, ch3_cq)

            with decimal.localcontext(self.d128_context):
                # type check before attempting to convert to decimal
                if ch1_cq is not None and ch1_cq and ch1_cq.to_decimal() <= self.ct_value_limit:
                    return True
                elif ch2_cq is not None and ch2_cq and ch2_cq.to_decimal() <= self.ct_value_limit:
                    return True
                elif ch3_cq is not None and ch3_cq and ch3_cq.to_decimal() <= self.ct_value_limit:
                    return True
                else:
                    return False
        else:
            return True


def current_filtered_positive_identifier() -> FilteredPositiveIdentifier:
    """Returns the current filtered positive identifier.

    Returns:
        {FilteredPositiveIdentifier} -- the current filtered positive identifier
    """
    return FilteredPositiveIdentifierV2()


def filtered_positive_identifier_by_version(version: str) -> FilteredPositiveIdentifier:
    """Returns the filtered positive identifier matching the specified version.

    Arguments:
        version {str} -- the filtered positive version

    Returns:
        {FilteredPositiveIdentifier} -- the matching filtered positive identifier
    """
    if version == FILTERED_POSITIVE_VERSION_0:
        return FilteredPositiveIdentifierV0()
    elif version == FILTERED_POSITIVE_VERSION_1:
        return FilteredPositiveIdentifierV1()
    elif version == FILTERED_POSITIVE_VERSION_2:
        return FilteredPositiveIdentifierV2()
    else:
        raise ValueError(f"'{version}' is not a known filtered positive version")


class FilteredPositiveIdentifierV0(FilteredPositiveIdentifier):
    def __init__(self):
        super(FilteredPositiveIdentifierV0, self).__init__()
        self.version = FILTERED_POSITIVE_VERSION_0


class FilteredPositiveIdentifierV1(FilteredPositiveIdentifier):
    def __init__(self):
        super(FilteredPositiveIdentifierV1, self).__init__()
        self.version = FILTERED_POSITIVE_VERSION_1
        self.root_sample_id_control_regex = re.compile("^CBIQA_")
        self.evaluate_ct_values = True


class FilteredPositiveIdentifierV2(FilteredPositiveIdentifier):
    def __init__(self):
        super(FilteredPositiveIdentifierV2, self).__init__()
        self.version = FILTERED_POSITIVE_VERSION_2
        self.root_sample_id_control_regex = re.compile("^(?:CBIQA_|QC0|ZZA000)")
        self.evaluate_ct_values = True
