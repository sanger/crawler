import logging
import logging.config
from datetime import datetime

from crawler.filtered_positive_identifier import current_filtered_positive_identifier
from crawler.helpers.cherrypicked_samples import filter_out_cherrypicked_samples
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


def run(settings_module: str = "", omit_dart: bool = False) -> None:
    """Updates filtered positive values for all positive samples in pending plates

    Arguments:
        settings_module {str} -- settings module from which to generate the app config
        omit_dart {bool} -- whether to omit DART queries/updates from the process
    """
    config, settings_module = get_config(settings_module)
    logging.config.dictConfig(config.LOGGING)

    logger.info("-" * 80)
    logger.info("STARTING FILTERED POSITIVES UPDATE")
    logger.info(f"Time start: {datetime.now()}")

    num_pending_plates = 0
    num_pos_samples = 0
    num_non_cp_pos_samples = 0
    mongo_updated = False
    mlwh_updated = False
    dart_updated = False
    try:
        samples = []
        if omit_dart:
            # Get positive result samples from Mongo
            logger.warning("Omitting DART from this update")
            samples = positive_result_samples_from_mongo(config)
        else:
            # Get barcodes of pending plates in DART
            logger.info("Selecting pending plates from DART...")
            pending_plate_barcodes = pending_plate_barcodes_from_dart(config)

            if num_pending_plates := len(pending_plate_barcodes):
                logger.info(f"{num_pending_plates} pending plates found in DART")

                # Get positive result samples from Mongo in these pending plates
                logger.info("Selecting postive samples in pending plates from Mongo...")
                samples = positive_result_samples_from_mongo(config, pending_plate_barcodes)
            else:
                logger.warning("No pending plates found in DART")

        if num_pos_samples := len(samples):
            logger.info(f"{num_pos_samples} matching positive samples found in Mongo")

            # Filter out cherrypicked samples
            logger.info("Filtering out cherrypicked samples...")
            non_cp_pos_pending_samples = filter_out_cherrypicked_samples(config, samples)

            if num_non_cp_pos_samples := len(non_cp_pos_pending_samples):
                logger.info(f"{num_non_cp_pos_samples} non-cherrypicked matching positive samples found")
                filtered_positive_identifier = current_filtered_positive_identifier()
                version = filtered_positive_identifier.version
                update_timestamp = datetime.utcnow()
                logger.info("Updating filtered positives...")
                update_filtered_positive_fields(
                    filtered_positive_identifier,
                    non_cp_pos_pending_samples,
                    version,
                    update_timestamp,
                )
                logger.info("Updated filtered positives")

                logger.info("Updating Mongo...")
                mongo_updated = update_mongo_filtered_positive_fields(
                    config, non_cp_pos_pending_samples, version, update_timestamp
                )
                logger.info("Finished updating Mongo")

                if mongo_updated:
                    logger.info("Updating MLWH...")
                    mlwh_updated = update_mlwh_filtered_positive_fields(config, non_cp_pos_pending_samples)
                    logger.info("Finished updating MLWH")

                    if not omit_dart and mlwh_updated:
                        logger.info("Updating DART...")
                        dart_updated = update_dart_fields(config, non_cp_pos_pending_samples)
                        logger.info("Finished updating DART")
            else:
                logger.warning("No non-cherrypicked matching positive samples found, not updating any database")
        else:
            logger.warning("No matching positive samples found in Mongo, not updating any database")

    except Exception as e:
        logger.error("---------- Process aborted: ----------")
        logger.error(f"An exception occurred, at {datetime.now()}")
        logger.exception(e)
    finally:
        dart_message = "DART omitted: True" if omit_dart else f"Found {num_pending_plates} pending plates in DART"
        logger.info(
            f"""
        ---------- Processing status of filtered positive rule changes: ----------
        -- {dart_message}
        -- Found {num_pos_samples} matching samples in Mongo
        -- Of which {num_non_cp_pos_samples} have not been cherrypicked
        -- Mongo updated: {mongo_updated}
        -- MLWH updated: {mlwh_updated}
        -- DART updated: {dart_updated}
        """
        )

    logger.info(f"Time finished: {datetime.now()}")
    logger.info("=" * 80)
