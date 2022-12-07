import logging
import logging.config
import sys
from typing import Tuple, cast

from lab_share_lib.config_readers import get_config

from crawler.types import Config
from migrations import (
    back_populate_source_plate_and_sample_uuids,
    back_populate_uuids_date_range,
    back_populate_uuids_plate_barcodes,
    reconnect_mlwh_with_mongo,
    update_dart,
    update_filtered_positives,
    update_legacy_filtered_positives,
    update_mlwh_with_legacy_samples,
)

config, settings_module = cast(Tuple[Config, str], get_config(""))

logger = logging.getLogger(__name__)
config.LOGGING["loggers"]["crawler"]["level"] = "DEBUG"
config.LOGGING["loggers"]["crawler"]["handlers"] = ["colored_stream"]
config.LOGGING["formatters"]["colored"][
    "format"
] = "{asctime:<15} {name:<60}:{lineno:<3} {log_color}{levelname:<7} {message}"
logging.config.dictConfig(config.LOGGING)

##
# Examples of how to run from command line:
# python run_migration.py sample_timestamps
# python run_migration.py update_mlwh_with_legacy_samples 200115_1200 200216_0900
# python run_migration.py update_mlwh_and_dart_with_legacy_samples 200115_1200 200216_0900
# python run_migration.py update_filtered_positives
##

print("Migration names:")
print("* update_mlwh_with_legacy_samples")
print("* update_mlwh_and_dart_with_legacy_samples")
print("* update_filtered_positives")
print("* update_legacy_filtered_positives")
print("* back_populate_uuids_date_range")
print("* back_populate_uuids_plate_barcodes")
print("* back_populate_source_plate_and_sample_uuids")
print("* reconnect_mlwh_with_mongo (ONLY RUN THIS MIGRATION IN TESTING ENVIRONMENTS)")


def migration_update_mlwh_with_legacy_samples():
    if not len(sys.argv) == 4:
        print(
            "Please add both start and end datetime range arguments for this migration "
            "(format YYMMDD_HHmm e.g. 200115_1200, inclusive), aborting"
        )
        return

    s_start_datetime = sys.argv[2]
    s_end_datetime = sys.argv[3]
    print("Running update_mlwh_with_legacy_samples migration")
    update_mlwh_with_legacy_samples.run(config, s_start_datetime=s_start_datetime, s_end_datetime=s_end_datetime)


def migration_update_mlwh_and_dart_with_legacy_samples():
    if not len(sys.argv) == 4:
        print(
            "Please add both start and end datetime range arguments for this migration "
            "(format YYMMDD_HHmm e.g. 200115_1200, inclusive), aborting"
        )
        return

    s_start_datetime = sys.argv[2]
    s_end_datetime = sys.argv[3]
    print("Running update_dart migration")
    update_dart.run(config, s_start_datetime=s_start_datetime, s_end_datetime=s_end_datetime)


def migration_update_filtered_positives():
    print("Running update_filtered_positives migration")
    omit_dart = sys.argv[2] == "omit_dart" if 2 < len(sys.argv) else False
    update_filtered_positives.run(omit_dart=omit_dart)


def migration_reconnect_mlwh_with_mongo():
    if not len(sys.argv) == 3:
        print("Please add a csv file, aborting")
        return

    csv_file = sys.argv[2]

    print("Running reconnect_mlwh_with_mongo")

    reconnect_mlwh_with_mongo.run(config, csv_file)


def migration_back_populate_source_plate_and_sample_uuids():
    if not len(sys.argv) == 3:
        print("Please add a csv file, aborting")
        return

    csv_file = sys.argv[2]

    print("Running back_populate_source_plate_and_sample_uuids")

    back_populate_source_plate_and_sample_uuids.run(config, csv_file)


def migration_back_populate_uuids_date_range():
    if not len(sys.argv) == 5:
        print(
            "Please add start and end datetime range arguments and the updated_at timestamp for this migration "
            "(start, end and updated_at datetime format YYMMDD_HHmm e.g. 200115_1200, inclusive), aborting"
        )
        return

    s_start_datetime = sys.argv[2]
    s_end_datetime = sys.argv[3]
    updated_at = sys.argv[4]

    print("Running back_populate_uuids_date_range")

    back_populate_uuids_date_range.run(
        config, s_start_datetime=s_start_datetime, s_end_datetime=s_end_datetime, updated_at=updated_at
    )


def migration_back_populate_uuids_plate_barcodes():
    if not len(sys.argv) == 3:
        print("Please add a csv file, aborting")
        return

    csv_file = sys.argv[2]

    print("Running back_populate_uuids_plate_barcodes")

    back_populate_uuids_plate_barcodes.run(config, s_filepath=csv_file)


def migration_update_legacy_filtered_positives():
    if not len(sys.argv) == 4:
        print(
            "Please add both start and end datetime range arguments for this migration "
            "(format YYMMDD_HHmm e.g. 200115_1200, inclusive), aborting"
        )
        return

    s_start_datetime = sys.argv[2]
    s_end_datetime = sys.argv[3]
    print("Running update_legacy_filtered_positives migration")
    update_legacy_filtered_positives.run(s_start_datetime=s_start_datetime, s_end_datetime=s_end_datetime)


def migration_by_name(migration_name):
    switcher = {
        "update_mlwh_with_legacy_samples": migration_update_mlwh_with_legacy_samples,
        "update_mlwh_and_dart_with_legacy_samples": migration_update_mlwh_and_dart_with_legacy_samples,
        "update_filtered_positives": migration_update_filtered_positives,
        "update_legacy_filtered_positives": migration_update_legacy_filtered_positives,
        "back_populate_uuids_date_range": migration_back_populate_uuids_date_range,
        "back_populate_uuids_plate_barcodes": migration_back_populate_uuids_plate_barcodes,
        "back_populate_source_plate_and_sample_uuids": migration_back_populate_source_plate_and_sample_uuids,
        "reconnect_mlwh_with_mongo": migration_reconnect_mlwh_with_mongo,
    }
    # Get the function from switcher dictionary
    func = switcher.get(migration_name, lambda: print("Invalid migration name, aborting"))
    # Execute the function
    func()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        migration_name = sys.argv[1]
        print(f"Migration name selected = {migration_name}")
        migration_by_name(migration_name)
    else:
        print("You must include a migration name as an argument after the command, aborting")
