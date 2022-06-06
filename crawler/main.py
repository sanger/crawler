import logging
import logging.config
import time

from crawler.config.centres import CENTRE_DATA_SOURCE_SFTP, get_centres_config
from crawler.constants import CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS, CENTRE_KEY_NAME, CENTRE_KEY_PREFIX
from crawler.db.mongo import create_mongo_client, get_mongo_db
from crawler.file_processing import Centre
from crawler.helpers.db_helpers import ensure_mongo_collections_indexed
from crawler.helpers.general_helpers import get_config
from crawler.priority_samples_process import update_priority_samples

logger = logging.getLogger(__name__)


def run(sftp: bool, keep_files: bool, add_to_dart: bool, settings_module: str = "", centre_prefix: str = "") -> None:
    try:
        start = time.time()
        config, settings_module = get_config(settings_module)

        logging.config.dictConfig(config.LOGGING)

        logger.info("-" * 80)
        logger.info("START")
        logger.info(f"Using settings from {settings_module}")

        # get or create the centres collection and filter down to only those with an SFTP data source
        centres = get_centres_config(config, CENTRE_DATA_SOURCE_SFTP)

        with create_mongo_client(config) as client:
            db = get_mongo_db(config, client)
            ensure_mongo_collections_indexed(db)

            if centre_prefix:
                # We are only interested in processing a single centre
                centres = list(filter(lambda config: config.get(CENTRE_KEY_PREFIX) == centre_prefix, centres))
            else:
                # We should only include centres that are to be batch processed
                centres = list(filter(lambda config: config.get(CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS, True), centres))

            centres_instances = [Centre(config, centre_config) for centre_config in centres]

            for centre_instance in centres_instances:
                logger.info("*" * 80)
                logger.info(f"Processing {centre_instance.centre_config[CENTRE_KEY_NAME]}")

                try:
                    if sftp:
                        centre_instance.download_csv_files()

                    centre_instance.process_files(add_to_dart)
                except Exception as e:
                    logger.error(f"Error in centre '{centre_instance.centre_config[CENTRE_KEY_NAME]}'")
                    logger.exception(e)
                finally:
                    if not keep_files and centre_instance.is_download_dir_walkable:
                        centre_instance.clean_up()

                # Prioritisation of samples
                update_priority_samples(db, config, add_to_dart)

        logger.info(f"Import complete in {round(time.time() - start, 2)}s")
        logger.info("=" * 80)
    except Exception as e:
        logger.exception(e)
