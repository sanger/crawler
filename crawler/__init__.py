import logging
import logging.config
import os
from http import HTTPStatus

from flask import Flask
from flask_apscheduler import APScheduler

from crawler.constants import SCHEDULER_JOB_ID_RUN_CRAWLER

scheduler = APScheduler()


def create_app(config_object: str = None) -> Flask:
    app = Flask(__name__)

    if config_object is None:
        app.config.from_object(os.environ["SETTINGS_MODULE"])
    else:
        app.config.from_object(config_object)

    # setup logging
    logging.config.dictConfig(app.config["LOGGING"])

    if app.config.get("CAN_CREATE_CHERRYPICKER_TEST_DATA", False):
        from crawler.blueprints import cherrypicker_test_data

        app.register_blueprint(cherrypicker_test_data.bp)

    if app.config.get("SCHEDULER_RUN", False):
        scheduler.init_app(app)
        scheduler.start()

    @app.get("/health")
    def health_check():
        if scheduler.get_job(SCHEDULER_JOB_ID_RUN_CRAWLER):
            return "Crawler is working", HTTPStatus.OK

        return "Crawler is not working correctly", HTTPStatus.INTERNAL_SERVER_ERROR

    return app
