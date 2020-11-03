import re
from crawler.constants import (
    POSITIVE_RESULT_VALUE,
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_CH1_CQ,
    FIELD_CH2_CQ,
    FIELD_CH3_CQ,
)

class TruePositiveIdentifier:
    # record/reference all versions and definitions here
    versions = [
      '1', # initial implementation, as per GPL-669
    ]
    result_regex = re.compile(F'^{POSITIVE_RESULT_VALUE}', re.IGNORECASE)
    root_sample_id_regex = re.compile('^CBIQA_')

    def current_version(self) -> str:
        """Returns the current version of the identifier.

            Returns:
                {str} - the version number
        """
        return self.versions[-1]

    def is_true_positive(self, doc_to_insert) -> bool:
        """Insert sample records into the mongo database from the parsed file information.

            Arguments:
                doc_to_insert {Dict[str, str]} -- information on a single sample extracted from csv files

            Returns:
                {bool} -- whether the sample is a true positive
        """

        if self.result_regex.match(doc_to_insert[FIELD_RESULT]) == None:
            return False
        
        if self.root_sample_id_regex.match(doc_to_insert[FIELD_ROOT_SAMPLE_ID]) is not None:
            return False

        ch1_cq = doc_to_insert[FIELD_CH1_CQ]
        ch2_cq = doc_to_insert[FIELD_CH2_CQ]
        ch3_cq = doc_to_insert[FIELD_CH3_CQ]
        if ch1_cq == None and ch2_cq == None and ch3_cq == None:
            return True
        elif ch1_cq <= 30 or ch2_cq <= 30 or ch3_cq <= 30:
            return True
        else:
            return False
        
