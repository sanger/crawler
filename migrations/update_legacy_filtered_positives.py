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
    all_mongo_samples,
    get_cherrypicked_samples,
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
        # Get all samples from Mongo
        logger.info("Selecting all samples from Mongo...")

        # If v0 version has been set on any samples, migration has likely been run before - do not want to run again in this case
        if v0_version_set(config) is False:
            samples = unmigrated_mongo_samples(config)

            root_sample_ids, plate_barcodes = extract_required_cp_info(samples)
            cp_samples_df = get_cherrypicked_samples(config, list(root_sample_ids), list(plate_barcodes))

            filtered_positive_identifier = FilteredPositiveIdentifier()
            version = filtered_positive_identifier.current_version()
            update_timestamp = datetime.now()

            update_filtered_positive_fields(
                filtered_positive_identifier,
                samples,
                version,
                update_timestamp,
            )

            mongo_updated = update_mongo_filtered_positive_fields(
                    config, samples, version, update_timestamp
                )

        else:
            logger.warning("v0 version has already been set in some Mongo samples. This migration has likely been run before.")
            raise
    except Exception as e:
        logger.error("---------- Process aborted: ----------")
        logger.error(f"An exception occurred, at {datetime.now()}")
        logger.exception(e)
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
