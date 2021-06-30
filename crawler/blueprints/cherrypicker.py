import logging

from flask import Blueprint, request
from flask_cors import CORS

from crawler.constants import FLASK_ERROR_UNEXPECTED, FLASK_ERROR_MISSING_PARAMETERS
from crawler.helpers.responses import bad_request, internal_server_error, accepted
# from crawler.jobs.cherrypicker import generate_test_data
from crawler.types import FlaskResponse

logger = logging.getLogger(__name__)

bp = Blueprint("cherrypicker", __name__)
CORS(bp)


@bp.patch("/cherrypicker_test_data/<run_id>")
def start_cherrypicker_test_data_generator_endpoint(run_id: str) -> FlaskResponse:
    """Generates cherrypicker test data for a number of plates with defined
    numbers of positives per plate.

    The body of the request should be:

    `{ "status": "started" }`

    It is expected that the run_id will already exist in Mongo DB with details
    of the plates to generate and that the status of the run will currently be
    "pending".

    Returns: FlaskResponse: metadata for the generated test data or a list of
        errors with the corresponding HTTP status code.
    """
    logger.info("Generating test data for cherrypicking hardware")

    try:
        if (
            (request_json := request.get_json()) is None or
            (new_status := request_json.get("status")) is None or
            new_status != "started"
        ):
            msg = f"{FLASK_ERROR_MISSING_PARAMETERS} - Request body should contain a JSON object with the key 'status' and value 'started'."
            logger.error(msg)
            return bad_request(msg)

        return accepted(run_id=run_id, status=new_status)

    except Exception as e:
        msg = f"{FLASK_ERROR_UNEXPECTED} ({type(e).__name__})"
        logger.error(msg)
        logger.exception(e)

        return internal_server_error(msg)
