from flask import current_app as app
import logging

from crawler import scheduler
from crawler.helpers.general_helpers import get_config
from crawler.main import run


logger = logging.getLogger(__name__)


def scheduled_run():
    """Scheduler's job to do a run every 30 minutes."""
    config, _ = get_config()
    logging.config.dictConfig(config.LOGGING)

    logger.info("Starting scheduled_run job.")

    with scheduler.app.app_context():
        use_sftp = app.config["USE_SFTP"]
        keep_files = app.config["KEEP_FILES"]
        add_to_dart = app.config["ADD_TO_DART"]
        run(use_sftp, keep_files, add_to_dart)
