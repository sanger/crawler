import logging
import logging.config
from http import HTTPStatus

from flask import Flask
from flask_apscheduler import APScheduler

scheduler = APScheduler()


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_envvar("SETTINGS_PATH")

    # setup logging
    logging.config.dictConfig(app.config["LOGGING"])

    if app.config.get("SCHEDULER_RUN", False):
        scheduler.init_app(app)
        scheduler.start()

    @app.get("/health")
    def health_check():
        return "Crawler is working", HTTPStatus.OK

    return app
