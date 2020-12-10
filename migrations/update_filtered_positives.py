import logging
import logging.config
from datetime import datetime

from crawler.filtered_positive_identifier import FilteredPositiveIdentifier
from crawler.helpers.general_helpers import get_config

from migrations.helpers.update_filtered_positives_helper import (
    pending_plate_barcodes_from_dart,
    positive_result_samples_from_mongo,
    update_dart_fields,
    update_filtered_positive_fields,
    update_mlwh_filtered_positive_fields,
    update_mongo_filtered_positive_fields,
)

logger = logging.getLogger(__name__)


def run(settings_module: str = "") -> None:
    """Updates filtered positive values for all positive samples in pending plates

    Arguments:
        config {ModuleType} -- application config specifying database details
    """
    config, settings_module = get_config(settings_module)
    logging.config.dictConfig(config.LOGGING)  # type: ignore

    logger.info("-" * 80)
    logger.info("STARTING FILTERED POSITIVES UPDATE")
    logger.info(f"Time start: {datetime.now()}")

    num_pending_plates = 0
    num_positive_pending_samples = 0
    mongo_updated = False
    mlwh_updated = False
    dart_updated = False
    try:
        # Get barcodes of pending plates in DART
        logger.info("Selecting pending plates from DART...")
        pending_plate_barcodes = pending_plate_barcodes_from_dart(config)
        if num_pending_plates := len(pending_plate_barcodes):
            logger.info(f"{len(pending_plate_barcodes)} pending plates found in DART")

            # Get positive result samples from Mongo in these pending plates
            logger.info("Selecting postive samples in pending plates from Mongo...")
            positive_pending_samples = positive_result_samples_from_mongo(config, pending_plate_barcodes)
            if num_positive_pending_samples := len(positive_pending_samples):
                logger.info(f"{num_positive_pending_samples} positive samples in pending plates found in Mongo")
                filtered_positive_identifier = FilteredPositiveIdentifier()
                version = filtered_positive_identifier.version()
                update_timestamp = datetime.now()
                logger.info("Updating filtered positives...")
                update_filtered_positive_fields(
                    filtered_positive_identifier,
                    positive_pending_samples,
                    version,
                    update_timestamp,
                )
                logger.info("Updated filtered positives")

                logger.info("Updating Mongo...")
                mongo_updated = update_mongo_filtered_positive_fields(
                    config, positive_pending_samples, version, update_timestamp
                )
                logger.info("Finished updating Mongo")

                if mongo_updated:
                    logger.info("Updating MLWH...")
                    mlwh_updated = update_mlwh_filtered_positive_fields(config, positive_pending_samples)
                    logger.info("Finished updating MLWH")

                    if mlwh_updated:
                        logger.info("Updating DART...")
                        dart_updated = update_dart_fields(config, positive_pending_samples)
                        logger.info("Finished updating DART")
            else:
                logger.warning("No positive samples in pending plates found in Mongo, not updating any database")
        else:
            logger.warning("No pending plates found in DART, not updating any database")

    except Exception as e:
        logger.error("---------- Process aborted: ----------")
        logger.error(f"An exception occurred, at {datetime.now()}")
        logger.exception(e)
    finally:
        logger.info(
            f"""
        ---------- Processing status of filtered positive rule changes: ----------
        -- Found {num_pending_plates} pending plates in DART
        -- Found {num_positive_pending_samples} samples in pending plates in Mongo
        -- Mongo updated: {mongo_updated}
        -- MLWH updated: {mlwh_updated}
        -- DART updated: {dart_updated}
        """
        )

    logger.info(f"Time finished: {datetime.now()}")
    logger.info("=" * 80)
