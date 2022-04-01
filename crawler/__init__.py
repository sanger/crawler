import atexit
import logging
import logging.config
import os
import threading
from http import HTTPStatus

from flask import Flask
from flask_apscheduler import APScheduler

from crawler.constants import SCHEDULER_JOB_ID_RUN_CRAWLER
from crawler.rabbit import reconnecting_consumer
from crawler.rabbit.reconnecting_consumer import ReconnectingConsumer

scheduler = APScheduler()
rabbit_consumer = ReconnectingConsumer("", "")
rabbit_consumer_thread = threading.Thread()


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

    start_rabbit_consumer()
    setup_routes(app)

    @app.get("/health")
    def _():
        """Checks the health of Crawler by checking that there is a scheduled job to run Crawler periodically"""
        if scheduler.get_job(SCHEDULER_JOB_ID_RUN_CRAWLER):
            return "Crawler is working", HTTPStatus.OK

        return "Crawler is not working correctly", HTTPStatus.INTERNAL_SERVER_ERROR

    return app


def setup_routes(app):
    if app.config.get("ENABLE_CHERRYPICKER_ENDPOINTS", False):
        from crawler.routes.v1 import routes as v1_routes

        app.register_blueprint(v1_routes.bp, url_prefix="/v1")


def start_rabbit_consumer():
    def run_consumer():
        rabbit_consumer.run()

    global rabbit_consumer_thread
    rabbit_consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    rabbit_consumer_thread.start()
