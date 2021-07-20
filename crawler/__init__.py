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

    if app.config.get("SCHEDULER_RUN", False):
        scheduler.init_app(app)
        scheduler.start()

    setup_blueprints(app)

    @app.get("/health")
    def _():
        if scheduler.get_job(SCHEDULER_JOB_ID_RUN_CRAWLER):
            return "Crawler is working", HTTPStatus.OK

        return "Crawler is not working correctly", HTTPStatus.INTERNAL_SERVER_ERROR

    return app


def setup_blueprints(app):
    if app.config.get("ENABLE_CHERRYPICKER_ENDPOINTS", False):
        from crawler.blueprints.v1 import cherrypicker_test_data as cptd_v1

        app.register_blueprint(cptd_v1.bp, url_prefix="/v1")
        app.register_blueprint(cptd_v1.bp)  # Also serve v1 at the root of the host for now
        # TODO: Remove the root API service when all calling services have been updated
