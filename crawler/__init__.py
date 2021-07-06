from http import HTTPStatus
import logging
import logging.config
import os

from flask import Flask
from flask_apscheduler import APScheduler

from crawler.constants import SCHEDULER_JOB_ID_RUN_CRAWLER

scheduler = APScheduler()


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_object(os.environ["SETTINGS_MODULE"])

    # setup logging
    logging.config.dictConfig(app.config["LOGGING"])

    if app.config.get("SCHEDULER_RUN", False):
        scheduler.init_app(app)
        scheduler.start()

    @app.get("/health")
    def health_check():
        if scheduler.get_job(SCHEDULER_JOB_ID_RUN_CRAWLER):
            return "Crawler is working", HTTPStatus.OK

        return "Crawler is not working correctly", HTTPStatus.INTERNAL_SERVER_ERROR

    return app
