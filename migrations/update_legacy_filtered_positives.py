import logging
import logging.config
import time
from datetime import datetime
from crawler.helpers.general_helpers import get_config
from migrations.helpers.update_filtered_positives_helper import (
    update_filtered_positive_fields,
    update_mlwh_filtered_positive_fields,
    update_mlwh_filtered_positive_fields_batch_query,
    update_mongo_filtered_positive_fields,
)
from migrations.helpers.update_legacy_filtered_positives_helper import (
    legacy_mongo_samples,
    get_cherrypicked_samples_by_date,
    v0_version_set,
    split_mongo_samples_by_version,
)
from crawler.constants import (
    V0_V1_CUTOFF_TIMESTAMP,
    V1_V2_CUTOFF_TIMESTAMP,
)
from crawler.filtered_positive_identifier import (
    FILTERED_POSITIVE_VERSION_0,
    FILTERED_POSITIVE_VERSION_1,
    FILTERED_POSITIVE_VERSION_2,
    filtered_positive_identifier_by_version,
)
from migrations.helpers.shared_helper import extract_required_cp_info

logger = logging.getLogger(__name__)

# Migration steps:
# 1. Get all legacy samples (those created in Mongo prior to Crawler
#    setting the filtered positive fields) from Mongo
# 2. Query to find which of these samples belong to v0, v1, v2 based on
#    when they were created in the 'sample' table of MLWH
# 3. Update the filtered positive fields of the samples using the correct
#    version rules
# 4. Update Mongo and MLWH with these filtered positive fields


def run(settings_module: str = "") -> None:
    """Migrate the existing samples to have the filtered positive values.

    Arguments:
        config {ModuleType} -- application config specifying database details
    """
    config, settings_module = get_config(settings_module)
    logging.config.dictConfig(config.LOGGING)  # type: ignore

    logger.info("-" * 80)
    logger.info("STARTING FILTERED POSITIVES LEGACY UPDATE")
    logger.info(f"Time start: {datetime.now()}")
    start_time = time.time()

    updated_key = "Updated"
    time_key = "Time taken"

    mongo_versions_updated = {
        FILTERED_POSITIVE_VERSION_0: { updated_key: False, time_key: 0.0 },
        FILTERED_POSITIVE_VERSION_1: { updated_key: False, time_key: 0.0 },
        FILTERED_POSITIVE_VERSION_2: { updated_key: False, time_key: 0.0 },
    }

    mlwh_versions_updated = {
        FILTERED_POSITIVE_VERSION_0: { updated_key: False, time_key: 0.0 },
        FILTERED_POSITIVE_VERSION_1: { updated_key: False, time_key: 0.0 },
        FILTERED_POSITIVE_VERSION_2: { updated_key: False, time_key: 0.0 },
    }

    try:
        continue_migration = True
        if v0_version_set(config):
            question = "v0 version has been set on some samples. This migration has likely been \
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
            logger.info("Selecting legacy samples from Mongo...")
            samples = legacy_mongo_samples(config)

            legacy_samples_num = len(samples)
            logger.info(f"{legacy_samples_num} samples found from Mongo")

            root_sample_ids, plate_barcodes = extract_required_cp_info(samples)

            logger.info("Getting v0 cherrypicked samples from MLWH")
            # Get v0 cherrypicked samples
            v0_cp_samples_df = get_cherrypicked_samples_by_date(
                config,
                list(root_sample_ids),
                list(plate_barcodes),
                "1970-01-01 00:00:01",
                V0_V1_CUTOFF_TIMESTAMP,
            )
            logger.debug(f"Found {len(v0_cp_samples_df.index)} v0 cherrypicked samples")

            logger.info("Getting v1 cherrypicked samples from MLWH")
            # Get v1 cherrypicked samples
            v1_cp_samples_df = get_cherrypicked_samples_by_date(
                config,
                list(root_sample_ids),
                list(plate_barcodes),
                V0_V1_CUTOFF_TIMESTAMP,
                V1_V2_CUTOFF_TIMESTAMP,
            )
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
                    mongo_versions_updated[version][time_key] = round(mongo_update_end_time - mongo_update_start_time, 2)

                    logger.info(f"Updating {version} filtered positives in MLWH...")
                    mlwh_update_start_time = time.time()

                    mlwh_updated = update_mlwh_filtered_positive_fields_batch_query(config, version_samples, version, update_timestamp)

                    if mlwh_updated:
                        logger.info(f"Finished updating {version} filtered positives in MLWH")
    
                        mlwh_update_end_time = time.time()
                        mlwh_versions_updated[version][updated_key] = True
                        mlwh_versions_updated[version][time_key] = round(mlwh_update_end_time - mlwh_update_start_time, 2)

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
