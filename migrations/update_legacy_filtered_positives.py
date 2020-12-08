import logging
import logging.config
from datetime import datetime
from crawler.filtered_positive_identifier import FilteredPositiveIdentifier
from crawler.helpers.general_helpers import get_config
from migrations.helpers.update_filtered_positives_helper import (
    update_filtered_positive_fields,
    update_mlwh_filtered_positive_fields,
    update_mongo_filtered_positive_fields,
    filtered_positive_fields_exist,
    all_mongo_samples,
)

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
        if not filtered_positive_fields_exist(config):
            # Get all samples from Mongo
            logger.info("Selecting all samples from Mongo...")
            samples = all_mongo_samples(config)

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
            logger.warning("Filtered positive fields already exist in MongoDB")
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
