import logging

from flask import Blueprint, request
from flask_cors import CORS

from crawler.constants import FLASK_ERROR_UNEXPECTED, FLASK_ERROR_MISSING_PARAMETERS
from crawler.helpers.responses import bad_request, internal_server_error, created
# from crawler.jobs.cherrypicker import generate_test_data
from crawler.types import FlaskResponse

logger = logging.getLogger(__name__)

bp = Blueprint("cherrypicker", __name__)
CORS(bp)


@bp.post("/cherrypicker/generate_test_data")
def generate_cherrypicker_test_data_endpoint() -> FlaskResponse:
    """Generates cherrypicker test data for a number of plates with defined
    numbers of positives per plate.

    The body of the request should be in the format:

    `{ "job_id": "CPTD-0000001" }`

    It is expected that this job ID will already exist in the Mongo DB with
    details of the plates to generate.

    Returns: FlaskResponse: metadata for the generated test data or a list of
        errors with the corresponding HTTP status code.
    """
    logger.info("Generating test data for cherrypicking hardware")

    try:
        if (
            (request_json := request.get_json()) is not None and
            (job_id := request_json.get("job_id")) is not None
        ):
            return created(job_id=job_id)

        msg = f"{FLASK_ERROR_MISSING_PARAMETERS} - Request body should contain a JSON object with a 'job_id' key."
        logger.error(msg)

        return bad_request(msg)
    except Exception as e:
        msg = f"{FLASK_ERROR_UNEXPECTED} ({type(e).__name__})"
        logger.error(msg)
        logger.exception(e)

        return internal_server_error(msg)
