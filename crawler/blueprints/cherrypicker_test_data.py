import logging

from datetime import datetime, timezone
from flask import Blueprint, request
from flask_cors import CORS

from crawler.constants import FLASK_ERROR_UNEXPECTED, FLASK_ERROR_MISSING_PARAMETERS
from crawler.helpers.responses import bad_request, internal_server_error, ok
from crawler.jobs.cherrypicker_test_data import TestDataError, process
from crawler.types import FlaskResponse

logger = logging.getLogger(__name__)

bp = Blueprint("cherrypicker", __name__)
CORS(bp)


@bp.post("/cherrypick-test-data")
def generate_test_data_endpoint() -> FlaskResponse:
    """Generates cherrypicker test data for a number of plates with defined
    numbers of positives per plate.

    The body of the request should be:

    `{ "run_id": "0123456789abcdef" }`

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
            (run_id := request_json.get("run_id")) is None
        ):
            msg = f"{FLASK_ERROR_MISSING_PARAMETERS} - Request body should contain a JSON object with a 'run_id' specified."
            logger.error(msg)
            return bad_request(msg)

        plates_meta = process(run_id)
        return ok(run_id=run_id, plates=plates_meta, status="completed")

    except Exception as e:
        if isinstance(e, TestDataError):
            msg = e.message
        else:
            msg = f"{FLASK_ERROR_UNEXPECTED} ({type(e).__name__})"

        logger.error(msg)
        logger.exception(e)

        timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

        return internal_server_error(msg, timestamp=timestamp)
