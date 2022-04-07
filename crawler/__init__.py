import logging
import logging.config
import os
from http import HTTPStatus

import flask
import werkzeug
from flask_apscheduler import APScheduler

from crawler.constants import SCHEDULER_JOB_ID_RUN_CRAWLER
from crawler.rabbit.background_consumer import BackgroundConsumer
from crawler.types import RabbitServerDetails

scheduler = APScheduler()


def create_app(config_object: str = None) -> flask.Flask:
    app = flask.Flask(__name__)

    if config_object is None:
        app.config.from_object(os.environ["SETTINGS_MODULE"])
    else:
        app.config.from_object(config_object)

    # setup logging
    logging.config.dictConfig(app.config["LOGGING"])

    if app.config.get("SCHEDULER_RUN", False):
        scheduler.init_app(app)
        scheduler.start()

    start_rabbit_consumer(app)
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


def start_rabbit_consumer(app):
    # Flask in debug mode spawns a child process so that it can restart the process each time your code changes,
    # the new child process initializes and starts a new consumer causing more than one to exist.
    if (flask.helpers.get_debug_flag() and not werkzeug.serving.is_running_from_reloader()) or not app.config[
        "RABBITMQ_HOST"
    ]:
        return

    rabbit_server = RabbitServerDetails(
        uses_ssl=app.config["RABBITMQ_SSL"],
        host=app.config["RABBITMQ_HOST"],
        port=app.config["RABBITMQ_PORT"],
        username=app.config["RABBITMQ_USERNAME"],
        password=app.config["RABBITMQ_PASSWORD"],
        vhost=app.config["RABBITMQ_VHOST"],
    )
    rabbit_queue = app.config["RABBITMQ_CRUD_QUEUE"]
    BackgroundConsumer(rabbit_server, rabbit_queue).start()
