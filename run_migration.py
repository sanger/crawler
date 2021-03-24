import logging
import logging.config
import sys

from crawler.helpers.general_helpers import get_config
from migrations import (
    sample_timestamps,
    update_dart,
    update_filtered_positives,
    update_legacy_filtered_positives,
    update_mlwh_with_legacy_samples,
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
##

print("Migration names:")
print("* sample_timestamps")
print("* update_mlwh_with_legacy_samples")
print("* update_mlwh_and_dart_with_legacy_samples")
print("* update_filtered_positives")
print("* update_legacy_filtered_positives")


def migration_sample_timestamps():
    print("Running sample_timestamps migration")
    sample_timestamps.run()


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
        "sample_timestamps": migration_sample_timestamps,
        "update_mlwh_with_legacy_samples": migration_update_mlwh_with_legacy_samples,
        "update_mlwh_and_dart_with_legacy_samples": migration_update_mlwh_and_dart_with_legacy_samples,
        "update_filtered_positives": migration_update_filtered_positives,
        "update_legacy_filtered_positives": migration_update_legacy_filtered_positives,
    }
    # Get the function from switcher dictionary
    func = switcher.get(migration_name, lambda: print("Invalid migration name, aborting"))
    # Execute the function
    func()


if len(sys.argv) > 1:
    migration_name = sys.argv[1]
    print(f"Migration name selected = {migration_name}")
    migration_by_name(migration_name)
else:
    print("You must include a migration name as an argument after the command, aborting")
