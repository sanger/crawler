import logging
import logging.config
import sys

from crawler.helpers.general_helpers import get_config
from migrations import (
    back_populate_uuids,
    update_dart,
    update_filtered_positives,
    update_legacy_filtered_positives,
    update_mlwh_with_legacy_samples,
    back_populate_source_plate_and_sample_uuids,
)

config, settings_module = get_config("")

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
# python run_migration.py back_populate_source_plate_and_sample_uuids ./mydir/mybarcodesfile.csv
##

print("Migration names:")
print("* update_mlwh_with_legacy_samples")
print("* update_mlwh_and_dart_with_legacy_samples")
print("* update_filtered_positives")
print("* update_legacy_filtered_positives")
print("* back_populate_uuids")
print("* back_populate_source_plate_and_sample_uuids")


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


def migration_back_populate_uuids():
    if not len(sys.argv) == 5:
        print(
            "Please add start and end datetime range arguments and the updated_at timestamp for this migration "
            "(start, end and updated_at datetime format YYMMDD_HHmm e.g. 200115_1200, inclusive), aborting"
        )
        return

    s_start_datetime = sys.argv[2]
    s_end_datetime = sys.argv[3]
    updated_at = sys.argv[4]

    print("Running back_populate_uuids")

    back_populate_uuids.run(
        config, s_start_datetime=s_start_datetime, s_end_datetime=s_end_datetime, updated_at=updated_at
    )


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

def migration_back_populate_source_plate_and_sample_uuids():
    if not len(sys.argv) == 3:
        print(
            "Please add a filepath argument to the csv file of barcodes for this migration "
            "(e.g. ./mydir/myfile.csv), aborting"
        )
        return

    s_filepath = sys.argv[2]
    print("Running back_populate_source_plate_and_sample_uuids migration")
    back_populate_source_plate_and_sample_uuids.run(config, s_filepath=s_filepath)

def migration_by_name(migration_name):
    switcher = {
        "update_mlwh_with_legacy_samples": migration_update_mlwh_with_legacy_samples,
        "update_mlwh_and_dart_with_legacy_samples": migration_update_mlwh_and_dart_with_legacy_samples,
        "update_filtered_positives": migration_update_filtered_positives,
        "update_legacy_filtered_positives": migration_update_legacy_filtered_positives,
        "back_populate_uuids": migration_back_populate_uuids,
        "back_populate_source_plate_and_sample_uuids": migration_back_populate_source_plate_and_sample_uuids,
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
