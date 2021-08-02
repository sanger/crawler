from flask import Blueprint

from crawler.routes.common.cherrypicker_test_data import generate_test_data_v1
from crawler.types import FlaskResponse

bp = Blueprint("v1_routes", __name__)


@bp.post("/cherrypick-test-data")
def generate_test_data_endpoint() -> FlaskResponse:
    return generate_test_data_v1()
