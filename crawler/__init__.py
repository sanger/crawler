from http import HTTPStatus
import logging
import logging.config
import os

from flask import Flask
from flask_apscheduler import APScheduler

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
        return "Crawler is working", HTTPStatus.OK

    return app
