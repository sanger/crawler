from migrations import (
  sample_timestamps,
  update_mlwh_with_legacy_samples,
)
import sys

##
# Examples of how to run from command line:
# python run_migration.py sample_timestamps
# python run_migration.py update_mlwh_with_legacy_samples 200115_1200 200216_0900
##

print("Migration names:")
print("* sample_timestamps")
print("* update_mlwh_with_legacy_samples")

def migration_sample_timestamps():
    print("Running sample_timestamps migration")
    sample_timestamps.run()

def migration_update_mlwh_with_legacy_samples():
    if not len(sys.argv) == 4:
        print("Please add both start and end datetime arguments for this migration (format yymmdd_hhmm e.g. 200115_1200), aborting")
        return

    s_start_datetime = sys.argv[2]
    s_end_datetime = sys.argv[3]
    print("Running update_mlwh_with_legacy_samples migration")
    update_mlwh_with_legacy_samples.run(s_start_datetime=s_start_datetime, s_end_datetime=s_end_datetime)

def migration_by_name(migration_name):
    switcher = {
        'sample_timestamps': migration_sample_timestamps,
        'update_mlwh_with_legacy_samples': migration_update_mlwh_with_legacy_samples,
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
    print(f"You must include a migration name as an argument after the command, aborting")
