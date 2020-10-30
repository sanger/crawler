class TruePositiveIdentifier:
    # record/reference all versions and definitions here
    versions = [
      '0' # used during initial development
    ]

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
        return True
