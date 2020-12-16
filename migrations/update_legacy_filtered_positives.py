import logging
import logging.config
from datetime import datetime
from crawler.helpers.general_helpers import get_config
from migrations.helpers.update_filtered_positives_helper import (
    update_filtered_positive_fields,
    update_mlwh_filtered_positive_fields,
    update_mongo_filtered_positive_fields,
)
from migrations.helpers.update_legacy_filtered_positives_helper import (
    unmigrated_mongo_samples,
    get_cherrypicked_samples_by_date,
    v0_version_set,
    split_mongo_samples_by_version,
    combine_samples,
)
from crawler.constants import (
    V0_V1_CUTOFF_TIMESTAMP,
    V1_V2_CUTOFF_TIMESTAMP,
)
from migrations.helpers.dart_samples_update_helper import extract_required_cp_info

logger = logging.getLogger(__name__)


def run(settings_module: str = "") -> None:
    """Migrate the existing samples to have the filtered positive values. Should only be run once.

    Arguments:
        config {ModuleType} -- application config specifying database details
    """
    config, settings_module = get_config(settings_module)
    logging.config.dictConfig(config.LOGGING)  # type: ignore

    logger.info("-" * 80)
    logger.info("STARTING FILTERED POSITIVES LEGACY UPDATE")
    logger.info(f"Time start: {datetime.now()}")

    mongo_updated = False
    mlwh_updated = False

    try:
        if v0_version_set(config):
            question = "v0 version has been set on some samples. This migration has likely been run before - do you still wish to proceed? (yes/no):"
            continue_migration = get_input(question)

            if continue_migration == "yes":
                pass
            elif continue_migration == "no":
                logger.info("Now exiting migration")
                raise Exception()
            else:
                logger.info("Invalid input, please enter 'yes' or 'no'. Now exiting migration")
                raise Exception()

        logger.info("Selecting unmigrated samples from Mongo...")
        samples = unmigrated_mongo_samples(config)

        if not samples:
            logger.info("All samples have filtered positive fields set, migration not needed.")
            raise Exception()

        root_sample_ids, plate_barcodes = extract_required_cp_info(samples)

        # Get v0 cherrypicked samples
        v0_cp_samples_df = get_cherrypicked_samples_by_date(
            config,
            list(root_sample_ids),
            list(plate_barcodes),
            '1970-01-01 00:00:01',
            V0_V1_CUTOFF_TIMESTAMP,
        )

        # Get v1 cherrypicked samples
        v1_cp_samples_df = get_cherrypicked_samples_by_date(
            config,
            list(root_sample_ids),
            list(plate_barcodes),
            V0_V1_CUTOFF_TIMESTAMP,
            V1_V2_CUTOFF_TIMESTAMP,
        )

        samples_by_version = split_mongo_samples_by_version(samples, v0_cp_samples_df, v1_cp_samples_df)

        update_timestamp = datetime.now()

        for filtered_positive_identifier, version_samples in samples_by_version.items():
            version = filtered_positive_identifier.version
            logger.info(f"Updating {version} filtered positives...")
            update_filtered_positive_fields(
                filtered_positive_identifier,
                version_samples,
                version,
                update_timestamp,
            )

        logger.info("Updated filtered positives")

        logger.info("Updating Mongo")

        for filtered_positive_identifier, version_samples in samples_by_version.items():
            logger.info(f"Updating {version} filtered positives in Mongo...")
            version = filtered_positive_identifier.version
            mongo_updated = update_mongo_filtered_positive_fields(
                config,
                version_samples,
                version,
                update_timestamp,
            )

        logger.info("Finished updating Mongo")

        all_versioned_samples = combine_samples(samples_by_version)

        if mongo_updated:
            logger.info("Updating MLWH...")
            mlwh_updated = update_mlwh_filtered_positive_fields(config, all_versioned_samples)
            logger.info("Finished updating MLWH")

    except Exception as e:
        logger.error("---------- Process aborted: ----------")
        logger.error(f"An exception occurred, at {datetime.now()}")
        logger.exception(e)
        raise
    finally:
        logger.info(
            f"""
        ---------- Processing status of filtered positive field migration: ----------
        -- Mongo updated: {mongo_updated}
        -- MLWH updated: {mlwh_updated}
        """
        )

    logger.info(f"Time finished: {datetime.now()}")
    logger.info("=" * 80)

def get_input(text):
    return input(text)
