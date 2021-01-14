import logging
import logging.config
import time
from datetime import datetime
from crawler.helpers.general_helpers import get_config
from migrations.helpers.update_filtered_positives_helper import (
    update_filtered_positive_fields,
    update_mlwh_filtered_positive_fields,
    update_mongo_filtered_positive_fields,
)
from migrations.helpers.update_legacy_filtered_positives_helper import (
    mongo_samples_by_date,
    get_cherrypicked_samples_by_date,
    filtered_positive_fields_set,
    split_mongo_samples_by_version,
    update_mlwh_filtered_positive_fields_batched,
)
from crawler.constants import (
    V0_V1_CUTOFF_TIMESTAMP,
    V1_V2_CUTOFF_TIMESTAMP,
    MONGO_DATETIME_FORMAT,
    FILTERED_POSITIVE_FIELDS_SET_DATE,
)
from crawler.filtered_positive_identifier import (
    FILTERED_POSITIVE_VERSION_0,
    FILTERED_POSITIVE_VERSION_1,
    FILTERED_POSITIVE_VERSION_2,
    filtered_positive_identifier_by_version,
)
from migrations.helpers.shared_helper import (
    extract_required_cp_info,
    valid_datetime_string,
)

logger = logging.getLogger(__name__)

# Migration steps:
# 1. Get all legacy samples (those created in Mongo prior to Crawler
#    setting the filtered positive fields) from Mongo
# 2. Query to find which of these samples belong to v0, v1, v2 based on
#    when they were created in the 'sample' table of MLWH
# 3. Update the filtered positive fields of the samples using the correct
#    version rules
# 4. Update Mongo and MLWH with these filtered positive fields


def run(settings_module: str = "", s_start_datetime: str = "", s_end_datetime: str = "") -> None:
    """Migrate the existing samples to have the filtered positive values.

    Arguments:
        settings_module {str} -- settings module from which to generate the app config
    """
    if not valid_datetime_string(s_start_datetime):
        logger.error("Aborting run: Expected format of Start datetime is YYMMDD_HHmm")
        return

    if not valid_datetime_string(s_end_datetime):
        logger.error("Aborting run: Expected format of End datetime is YYMMDD_HHmm")
        return

    start_datetime = datetime.strptime(s_start_datetime, MONGO_DATETIME_FORMAT)
    end_datetime = datetime.strptime(s_end_datetime, MONGO_DATETIME_FORMAT)
    fields_set_datetime = datetime.strptime(FILTERED_POSITIVE_FIELDS_SET_DATE, "%Y-%m-%d")

    if start_datetime > end_datetime:
        logger.error("Aborting run: End datetime must be greater than Start datetime")
        return

    if not end_datetime <= fields_set_datetime:
        logger.error("Aborting run: Date range must be prior to the 17th December")
        return

    config, settings_module = get_config(settings_module)
    logging.config.dictConfig(config.LOGGING)  # type: ignore

    logger.info("-" * 80)
    logger.info("STARTING FILTERED POSITIVES LEGACY UPDATE")
    logger.info(f"Time start: {datetime.now()}")
    start_time = time.time()

    updated_key = "Updated"
    time_key = "Time taken"

    mongo_versions_updated = {
        FILTERED_POSITIVE_VERSION_0: {updated_key: False, time_key: 0.0},
        FILTERED_POSITIVE_VERSION_1: {updated_key: False, time_key: 0.0},
        FILTERED_POSITIVE_VERSION_2: {updated_key: False, time_key: 0.0},
    }

    mlwh_versions_updated = {
        FILTERED_POSITIVE_VERSION_0: {updated_key: False, time_key: 0.0},
        FILTERED_POSITIVE_VERSION_1: {updated_key: False, time_key: 0.0},
        FILTERED_POSITIVE_VERSION_2: {updated_key: False, time_key: 0.0},
    }

    try:
        continue_migration = True

        logger.info("Checking whether filtered positive version has been set on any samples...")
        if filtered_positive_fields_set(config, start_datetime, end_datetime):
            question = "The filtered positive field has been set on some samples. This migration has likely been \
run before - do you still wish to proceed? (yes/no):"
            response = get_input(question)

            if response == "yes":
                pass
            elif response == "no":
                continue_migration = False
            else:
                logger.warning("Invalid input, please enter 'yes' or 'no'. Now exiting migration")
                continue_migration = False

        if continue_migration:
            logger.info(f"Selecting legacy samples from Mongo between {start_datetime} and {end_datetime}...")
            samples = mongo_samples_by_date(config, start_datetime, end_datetime)

            legacy_samples_num = len(samples)
            logger.info(f"{legacy_samples_num} samples found from Mongo")

            root_sample_ids, plate_barcodes = extract_required_cp_info(samples)

            logger.info("Querying for v0 cherrypicked samples from MLWH")
            # Get v0 cherrypicked samples
            v0_cp_samples_df = get_cherrypicked_samples_by_date(
                config,
                list(root_sample_ids),
                list(plate_barcodes),
                "1970-01-01 00:00:01",
                V0_V1_CUTOFF_TIMESTAMP,
            )
            if v0_cp_samples_df is None:
                raise Exception("Unable to determine cherry-picked sample - potentially error connecting to MySQL")

            logger.debug(f"Found {len(v0_cp_samples_df.index)} v0 cherrypicked samples")

            logger.info("Querying for cherrypicked samples from MLWH")
            # Get v1 cherrypicked samples
            v1_cp_samples_df = get_cherrypicked_samples_by_date(
                config,
                list(root_sample_ids),
                list(plate_barcodes),
                V0_V1_CUTOFF_TIMESTAMP,
                V1_V2_CUTOFF_TIMESTAMP,
            )
            if v1_cp_samples_df is None:
                raise Exception("Unable to determine cherry-picked sample - potentially error connecting to MySQL")

            logger.debug(f"Found {len(v1_cp_samples_df.index)} v1 cherrypicked samples")

            logger.info("Splitting samples by version...")
            samples_by_version = split_mongo_samples_by_version(samples, v0_cp_samples_df, v1_cp_samples_df)

            update_timestamp = datetime.now()

            for version, version_samples in samples_by_version.items():
                filtered_positive_identifier = filtered_positive_identifier_by_version(version)
                logger.info(f"Updating {version} filtered positives...")
                update_filtered_positive_fields(
                    filtered_positive_identifier,
                    version_samples,
                    version,
                    update_timestamp,
                )

            logger.info("Updated filtered positives")

            logger.info("Updating Mongo")

            for version, version_samples in samples_by_version.items():
                logger.info(f"Updating {version} filtered positives in Mongo, total {len(version_samples)} records...")
                mongo_update_start_time = time.time()
                mongo_updated = update_mongo_filtered_positive_fields(
                    config,
                    version_samples,
                    version,
                    update_timestamp,
                )
                if mongo_updated:
                    logger.info(f"Finished updating {version} filtered positives in Mongo")

                    mongo_update_end_time = time.time()
                    mongo_versions_updated[version][updated_key] = True
                    mongo_versions_updated[version][time_key] = round(
                        mongo_update_end_time - mongo_update_start_time, 2
                    )

                    logger.info(f"Updating {version} filtered positives in MLWH...")
                    mlwh_update_start_time = time.time()

                    mlwh_updated = update_mlwh_filtered_positive_fields_batched(
                        config, version_samples, version, update_timestamp
                    )

                    if mlwh_updated:
                        logger.info(f"Finished updating {version} filtered positives in MLWH")

                        mlwh_update_end_time = time.time()
                        mlwh_versions_updated[version][updated_key] = True
                        mlwh_versions_updated[version][time_key] = round(
                            mlwh_update_end_time - mlwh_update_start_time, 2
                        )

            logger.info("Finished updating databases")
        else:
            logger.info("Now exiting migration")
    except Exception as e:
        logger.error("---------- Process aborted: ----------")
        logger.error(f"An exception occurred, at {datetime.now()}")
        logger.exception(e)
        raise
    finally:
        end_time = time.time()
        logger.info(
            f"""
        ---------- Processing status of filtered positive field migration: ----------
        -- Mongo updated with v0 filtered positives: \
{mongo_versions_updated[FILTERED_POSITIVE_VERSION_0][updated_key]}, \
time taken: \
{mongo_versions_updated[FILTERED_POSITIVE_VERSION_0][time_key]}s
        -- Mongo updated with v1 filtered positives: \
{mongo_versions_updated[FILTERED_POSITIVE_VERSION_1][updated_key]}, \
time taken: \
{mongo_versions_updated[FILTERED_POSITIVE_VERSION_1][time_key]}s
        -- Mongo updated with v2 filtered positives: \
{mongo_versions_updated[FILTERED_POSITIVE_VERSION_2][updated_key]}, \
time taken: \
{mongo_versions_updated[FILTERED_POSITIVE_VERSION_2][time_key]}s
        -- MLWH updated with v0 filtered positives: \
{mlwh_versions_updated[FILTERED_POSITIVE_VERSION_0][updated_key]}, \
time taken: \
{mlwh_versions_updated[FILTERED_POSITIVE_VERSION_0][time_key]}s
        -- MLWH updated with v1 filtered positives: \
{mlwh_versions_updated[FILTERED_POSITIVE_VERSION_1][updated_key]}, \
time taken: \
{mlwh_versions_updated[FILTERED_POSITIVE_VERSION_1][time_key]}s
        -- MLWH updated with v2 filtered positives: \
{mlwh_versions_updated[FILTERED_POSITIVE_VERSION_2][updated_key]}, \
time taken: \
{mlwh_versions_updated[FILTERED_POSITIVE_VERSION_2][time_key]}s
        """
        )

    logger.info(f"Time finished: {datetime.now()}")
    logger.info(f"Migration complete in {round(end_time - start_time, 2)}s")
    logger.info("=" * 80)


def get_input(text):
    return input(text)
