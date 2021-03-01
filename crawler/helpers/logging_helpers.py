from crawler.helpers.enums import ErrorLevel


class AggregateTypeBase:
    """Base class for Aggregate types. Should not be instantiated directly."""

    def __init__(self):
        self.error_level = ErrorLevel.DEBUG
        self.count_errors = 0
        self.max_errors = -1
        self.message = ""
        self.short_display_description = ""
        self.type_str = ""

    def add_error(self, message: str) -> None:
        """Adds a new error to the aggregate type. Checks max_errors to decide whether message
        should be appended to the default message or not. Increments total counter for this type of
        error.

        Arguments:
            message {str} -- the specific message for this error e.g. with a line number or barcode
        """
        self.count_errors += 1
        if self.max_errors > 0 and self.count_errors <= self.max_errors:
            self.message = self.message + f" (e.g. {message})"

    def get_message(self):
        return self.message

    def get_report_message(self):
        return f"Total number of {self.short_display_description} errors ({self.type_str}): {self.count_errors}"


# See confluence for full table of aggregate types
# https://ssg-confluence.internal.sanger.ac.uk/display/PSDPUB/i.+Low+Occupancy+Cherry+Picking


class AggregateType1(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 1"
        self.error_level = ErrorLevel.DEBUG
        self.message = f"{self.error_level.name}: Blank rows in files. ({self.type_str})"
        self.short_display_description = "Blank row"


class AggregateType2(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 2"
        self.error_level = ErrorLevel.CRITICAL
        self.message = (
            f"{self.error_level.name}: Files where we do not have the expected main column headers of "
            f"Root Sample ID, RNA ID and Result. ({self.type_str})"
        )
        self.short_display_description = "Missing header column"


class AggregateType3(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 3"
        self.error_level = ErrorLevel.WARNING
        self.message = (
            f"{self.error_level.name}: Sample rows that have Root Sample ID value but no other "
            f"information. ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "Only root sample id"


class AggregateType4(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 4"
        self.error_level = ErrorLevel.ERROR
        self.message = (
            f"{self.error_level.name}: Sample rows that have Root Sample ID and Result values but no "
            f"RNA ID (no plate barcode). ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "No plate barcode"


class AggregateType5(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 5"
        self.error_level = ErrorLevel.WARNING
        self.message = f"{self.error_level.name}: Duplicates detected within the file. ({self.type_str})"
        self.max_errors = 5
        self.short_display_description = "Duplicates within file"


class AggregateType6(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 6"
        self.error_level = ErrorLevel.WARNING
        self.message = (
            f"{self.error_level.name}: Duplicates detected matching rows in previous files. ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "Duplicates to previous files"


class AggregateType7(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 7"
        self.error_level = ErrorLevel.WARNING
        self.message = (
            f"{self.error_level.name}: Samples rows matching previously uploaded rows but with "
            f"different test date. ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "Different test date"


# Type 8 is valid and not logged (re-tests of samples)


class AggregateType9(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 9"
        self.error_level = ErrorLevel.CRITICAL
        self.message = (
            f"{self.error_level.name}: Sample rows failing to match expected format (regex) for "
            f"RNA ID field. ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "Failed regex on plate barcode"


class AggregateType10(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 10"
        self.error_level = ErrorLevel.CRITICAL
        self.message = f"{self.error_level.name}: File is unexpected type and cannot be processed. ({self.type_str})"
        self.max_errors = -1
        self.short_display_description = "File wrong type"


# Type 11 is blacklisted file, not logged


class AggregateType12(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 12"
        self.error_level = ErrorLevel.ERROR
        self.message = f"{self.error_level.name}: Sample rows that do not contain a Lab ID. ({self.type_str})"
        self.max_errors = 5
        self.short_display_description = "No Lab ID"


class AggregateType13(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 13"
        self.error_level = ErrorLevel.WARNING
        self.message = f"{self.error_level.name}: Sample rows that contain unexpected columns. ({self.type_str})"
        self.max_errors = 5
        self.short_display_description = "Extra column(s)"


class AggregateType14(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 14"
        self.error_level = ErrorLevel.CRITICAL
        self.message = f"{self.error_level.name}: Files where the MLWH database insert has failed. ({self.type_str})"
        self.short_display_description = "Failed MLWH inserts"


class AggregateType15(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 15"
        self.error_level = ErrorLevel.CRITICAL
        self.message = (
            f"{self.error_level.name}: Files where the MLWH database connection could not be made. ({self.type_str})"
        )
        self.short_display_description = "Failed MLWH connection"


class AggregateType16(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 16"
        self.error_level = ErrorLevel.ERROR
        self.message = f"{self.error_level.name}: Sample rows that have an invalid Result value. ({self.type_str})"
        self.max_errors = 5
        self.short_display_description = "Invalid Result value"


class AggregateType17(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 17"
        self.error_level = ErrorLevel.ERROR
        self.message = (
            f"{self.error_level.name}: Sample rows that have an invalid CT channel Target value. ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "Invalid CHn-Target value"


class AggregateType18(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 18"
        self.error_level = ErrorLevel.ERROR
        self.message = (
            f"{self.error_level.name}: Sample rows that have an invalid CT channel Result value. ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "Invalid CHn-Result value"


class AggregateType19(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 19"
        self.error_level = ErrorLevel.ERROR
        self.message = (
            f"{self.error_level.name}: Sample rows that have an invalid CT channel Cq value. ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "Invalid CHn-Cq value"


class AggregateType20(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 20"
        self.error_level = ErrorLevel.ERROR
        self.message = (
            f"{self.error_level.name}: Sample rows that have a CHn-Cq value "
            f"out of range (0..100). ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "Out of range CHn-Cq value"


class AggregateType21(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 21"
        self.error_level = ErrorLevel.ERROR
        self.message = (
            f"{self.error_level.name}: Sample rows where a Positive Result value does not align with CT channel "
            f"Results. ({self.type_str})"
        )
        self.max_errors = 5
        self.short_display_description = "Result not aligned with CHn-Results"


class AggregateType22(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 22"
        self.error_level = ErrorLevel.ERROR
        self.message = (
            "{self.error_level.name}: Files where the DART database inserts have failed for some plates. "
            f"({self.type_str})"
        )
        self.short_display_description = "Failed DART plate inserts"


class AggregateType23(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 23"
        self.error_level = ErrorLevel.CRITICAL
        self.message = f"{self.error_level.name}: Files where all DART database inserts have failed. ({self.type_str})"
        self.short_display_description = "Failed DART file inserts"


class AggregateType24(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 24"
        self.error_level = ErrorLevel.CRITICAL
        self.message = (
            f"{self.error_level.name}: Files where the DART database connection could not be made. ({self.type_str})"
        )
        self.short_display_description = "Failed DART connection"


class AggregateType25(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 25"
        self.error_level = ErrorLevel.ERROR
        self.message = (
            f"{self.error_level.name}: Found duplicate source plate barcodes from different labs ({self.type_str})"
        )
        self.short_display_description = "Duplicate source plate barcodes from different labs"


class AggregateType26(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 26"
        self.error_level = ErrorLevel.CRITICAL
        self.message = (
            f"{self.error_level.name}: Files where source plate UUIDs could not be assigned to any sample "
            f"({self.type_str})"
        )
        self.short_display_description = "Failed assigning source plate UUIDs"


class AggregateType27(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 27"
        self.error_level = ErrorLevel.ERROR
        self.message = f"{self.error_level.name}: Date field has an unknown date format." f"({self.type_str})"
        self.short_display_description = "Unknown date format"


class AggregateType28(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 28"
        self.error_level = ErrorLevel.CRITICAL
        self.message = (
            f"{self.error_level.name}: Priority samples where the MLWH database "
            f"insert has failed. ({self.type_str})"
        )
        self.short_display_description = "Priority samples - Failed MLWH inserts"


class AggregateType29(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 29"
        self.error_level = ErrorLevel.CRITICAL
        self.message = (
            f"{self.error_level.name}: Priority samples where the MLWH database connection "
            f"could not be made. ({self.type_str})"
        )
        self.short_display_description = "Priority samples - Failed MLWH connection"


class AggregateType30(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 30"
        self.error_level = ErrorLevel.CRITICAL
        self.message = (
            f"{self.error_level.name}: Priority samples where all DART database inserts have failed. ({self.type_str})"
        )
        self.short_display_description = "Priority samples - Failed DART inserts"


class AggregateType31(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 31"
        self.error_level = ErrorLevel.CRITICAL
        self.message = (
            f"{self.error_level.name}: Priority samples where the DART database connection "
            f"could not be made. ({self.type_str})"
        )
        self.short_display_description = "Priority samples - Failed priority DART connection"


class AggregateType32(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 32"
        self.error_level = ErrorLevel.CRITICAL
        self.message = (
            f"{self.error_level.name}: Priority samples that we have in Mongodb "
            f"but they are still unprocessed. ({self.type_str})"
        )
        self.short_display_description = "Priority samples - Validation failure"


class AggregateType33(AggregateTypeBase):
    def __init__(self):
        super().__init__()
        self.type_str = "TYPE 33"
        self.error_level = ErrorLevel.ERROR
        self.message = (
            "{self.error_level.name}: Priority samples where the DART database inserts have failed for some plates. "
            f"({self.type_str})"
        )
        self.short_display_description = "Priority samples - Failed DART plate inserts"


# Class to handle logging of errors of the various types per file
class LoggingCollection:
    def __init__(self):
        self.aggregator_types = {
            "TYPE 1": AggregateType1(),
            "TYPE 2": AggregateType2(),
            "TYPE 3": AggregateType3(),
            "TYPE 4": AggregateType4(),
            "TYPE 5": AggregateType5(),
            "TYPE 6": AggregateType6(),
            "TYPE 7": AggregateType7(),
            "TYPE 9": AggregateType9(),
            "TYPE 10": AggregateType10(),
            "TYPE 12": AggregateType12(),
            "TYPE 13": AggregateType13(),
            "TYPE 14": AggregateType14(),
            "TYPE 15": AggregateType15(),
            "TYPE 16": AggregateType16(),
            "TYPE 17": AggregateType17(),
            "TYPE 18": AggregateType18(),
            "TYPE 19": AggregateType19(),
            "TYPE 20": AggregateType20(),
            "TYPE 21": AggregateType21(),
            "TYPE 22": AggregateType22(),
            "TYPE 23": AggregateType23(),
            "TYPE 24": AggregateType24(),
            "TYPE 25": AggregateType25(),
            "TYPE 26": AggregateType26(),
            "TYPE 27": AggregateType27(),
            "TYPE 28": AggregateType28(),
            "TYPE 29": AggregateType29(),
            "TYPE 30": AggregateType30(),
            "TYPE 31": AggregateType31(),
            "TYPE 32": AggregateType32(),
            "TYPE 33": AggregateType33(),
        }

    def add_error(self, aggregate_error_type, message):
        self.aggregator_types[aggregate_error_type].add_error(message)

    def get_aggregate_messages(self):
        msgs = []
        for (_k, v) in sorted(self.aggregator_types.items()):
            if v.count_errors > 0:
                msgs.append(v.get_message())

        return msgs

    def get_aggregate_total_messages(self):
        msgs = []
        for (_k, v) in sorted(self.aggregator_types.items()):
            if v.count_errors > 0:
                msgs.append(v.get_report_message())

        return msgs

    def get_messages_for_import(self):
        return self.get_aggregate_total_messages() + self.get_aggregate_messages()

    def get_count_of_all_errors_and_criticals(self):
        count = 0
        for (_k, v) in self.aggregator_types.items():
            if v.error_level == ErrorLevel.ERROR or v.error_level == ErrorLevel.CRITICAL:
                count += v.count_errors

        return count
