from crawler.helpers import get_config
from datetime import datetime
from migrations.helpers.update_filtered_positives_helper import (
    pending_plate_barcodes_from_dart,
    positive_result_samples_from_mongo,
    update_filtered_positive_fields,
    update_mongo_filtered_positive_fields,
    update_mlwh_filtered_positive_fields,
    update_dart_filtered_positive_fields,
)
from migrations.helpers.shared_helper import print_exception
from crawler.filtered_positive_identifier import FilteredPositiveIdentifier


def run(settings_module: str = "") -> None:
    """Updates filtered positive values for all positive samples in pending plates

    Arguments:
        config {ModuleType} -- application config specifying database details
    """
    print("-" * 80)
    print("STARTING FILTERED POSITIVES UPDATE")
    print(f"Time start: {datetime.now()}")

    config, settings_module = get_config(settings_module)
    num_pending_plates = 0
    num_positive_pending_samples = 0
    mongo_updated = False
    mlwh_updated = False
    dart_updated = False
    try:
        # Get barcodes of pending plates in DART
        print("Selecting pending plates from DART...")
        pending_plate_barcodes = pending_plate_barcodes_from_dart(config)
        num_pending_plates = len(pending_plate_barcodes)
        print(f"{len(pending_plate_barcodes)} pending plates found in DART")

        if num_pending_plates > 0:
            # Get positive result samples from Mongo in these pending plates
            print("Selecting postive samples in pending plates from Mongo...")
            positive_pending_samples = positive_result_samples_from_mongo(
                config, pending_plate_barcodes
            )
            num_positive_pending_samples = len(positive_pending_samples)
            print(
                f"{num_positive_pending_samples} positive samples in pending plates found in Mongo"
            )

            if num_positive_pending_samples > 0:
                filtered_positive_identifier = FilteredPositiveIdentifier()
                version = filtered_positive_identifier.current_version()
                update_timestamp = datetime.now()
                print("Updating filtered positives...")
                update_filtered_positive_fields(
                    filtered_positive_identifier,
                    positive_pending_samples,
                    version,
                    update_timestamp,
                )
                print("Updated filtered positives")

                print("Updating Mongo...")
                mongo_updated = update_mongo_filtered_positive_fields(
                    config, positive_pending_samples, version, update_timestamp
                )
                print("Finished updating Mongo")

                if mongo_updated:
                    print("Updating MLWH...")
                    mlwh_updated = update_mlwh_filtered_positive_fields(
                        config, positive_pending_samples
                    )
                    print("Finished updating MLWH")

                    if mlwh_updated:
                        print("Updating DART...")
                        dart_updated = update_dart_filtered_positive_fields(
                            config, positive_pending_samples
                        )
                        print("Finished updating DART")
            else:
                print(
                    "No positive samples in pending plates found in Mongo, not updating any database"
                )
        else:
            print("No pending plates found in DART, not updating any database")

    except Exception as e:
        print("---------- Process aborted: ----------")
        print_exception()
    finally:
        print("---------- Processing status of filtered positive rule changes: ----------")
        print(f"-- Found {num_pending_plates} pending plates in DART")
        print(f"-- Found {num_positive_pending_samples} samples in pending plates in Mongo")
        print(f"-- Mongo updated: {mongo_updated}")
        print(f"-- MLWH updated: {mlwh_updated}")
        print(f"-- DART updated: {dart_updated}")

    print(f"Time finished: {datetime.now()}")
    print("=" * 80)
