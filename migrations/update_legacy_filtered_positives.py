import logging
import logging.config
from datetime import datetime
from crawler.filtered_positive_identifier import FilteredPositiveIdentifier
from crawler.helpers.general_helpers import get_config
from migrations.helpers.update_filtered_positives_helper import (
    update_filtered_positive_fields,
    update_mlwh_filtered_positive_fields,
    update_mongo_filtered_positive_fields,
)
from migrations.helpers.update_legacy_filtered_positives_helper import (
    unmigrated_mongo_samples,
    get_v0_cherrypicked_samples,
    v0_version_set,
    split_v0_cherrypicked_mongo_samples,
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
        # If v0 version has been set on any samples, migration has likely been run before - do not
        # want to run in this case
        if v0_version_set(config) is False:
            logger.info("Selecting unmigrated samples from Mongo...")
            samples = unmigrated_mongo_samples(config)

            if samples is None:
                logger.info("All samples have filtered positive fields set, migration not needed.")
                raise Exception()

            root_sample_ids, plate_barcodes = extract_required_cp_info(samples)

            cp_samples_df = get_v0_cherrypicked_samples(
                config,
                list(root_sample_ids),
                list(plate_barcodes)
            )

            v0_unmigrated_samples, v1_unmigrated_samples = split_v0_cherrypicked_mongo_samples(
                                                            samples, cp_samples_df
                                                          )

            # Updating v0 filtered positive fields
            filtered_positive_identifier = FilteredPositiveIdentifier("v0")
            version = filtered_positive_identifier.current_version()
            update_timestamp = datetime.now()

            logger.info("Updating v0 filtered positives...")
            update_filtered_positive_fields(
                filtered_positive_identifier,
                v0_unmigrated_samples,
                version,
                update_timestamp,
            )

            # Updating v1 filtered positive fields
            filtered_positive_identifier = FilteredPositiveIdentifier("v1")
            version = filtered_positive_identifier.current_version()
            update_timestamp = datetime.now()

            logger.info("Updating v1 filtered positives...")
            update_filtered_positive_fields(
                filtered_positive_identifier,
                v1_unmigrated_samples,
                version,
                update_timestamp,
            )
            logger.info("Updated filtered positives")

            migrated_samples = v0_unmigrated_samples + v1_unmigrated_samples
            logger.info("Updating Mongo...")
            mongo_updated = update_mongo_filtered_positive_fields(
                config,
                migrated_samples,
                version,
                update_timestamp
            )
            logger.info("Finished updating Mongo")

            if mongo_updated:
                logger.info("Updating MLWH...")
                mlwh_updated = update_mlwh_filtered_positive_fields(config, migrated_samples)
                logger.info("Finished updating MLWH")

        else:
            logger.warning(
                "v0 version has already been set in some Mongo samples - this migration has likely been run before."
            )
            raise Exception()
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
