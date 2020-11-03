import re
import decimal
from bson.decimal128 import create_decimal128_context
from crawler.constants import (
    POSITIVE_RESULT_VALUE,
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_CH1_CQ,
    FIELD_CH2_CQ,
    FIELD_CH3_CQ,
)

class FilteredPositiveIdentifier:
    # record/reference all versions and definitions here
    versions = [
      'v1', # initial implementation, as per GPL-669
    ]
    result_regex = re.compile(F'^{POSITIVE_RESULT_VALUE}', re.IGNORECASE)
    root_sample_id_regex = re.compile('^CBIQA_')
    ct_value_limit = decimal.Decimal(30)
    d128_context = create_decimal128_context()

    def current_version(self) -> str:
        """Returns the current version of the identifier.

            Returns:
                {str} - the version number
        """
        return self.versions[-1]

    def is_positive(self, doc_to_insert) -> bool:
        """Determines whether a sample is a filtered positive.

            Arguments:
                doc_to_insert {Dict[str, str]} -- information on a single sample extracted from csv files

            Returns:
                {bool} -- whether the sample is a filtered positive
        """
        if self.result_regex.match(doc_to_insert[FIELD_RESULT]) == None:
            return False
        
        if self.root_sample_id_regex.match(doc_to_insert[FIELD_ROOT_SAMPLE_ID]) is not None:
            return False

        ch1_cq = doc_to_insert.get(FIELD_CH1_CQ)
        ch2_cq = doc_to_insert.get(FIELD_CH2_CQ)
        ch3_cq = doc_to_insert.get(FIELD_CH3_CQ)
        if ch1_cq == None and ch2_cq == None and ch3_cq == None:
            return True
        
        with decimal.localcontext(self.d128_context):
            if ch1_cq.to_decimal() <= self.ct_value_limit or ch2_cq.to_decimal() <= self.ct_value_limit or ch3_cq.to_decimal() <= self.ct_value_limit:
                return True
            else:
                return False
