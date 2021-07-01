from http import HTTPStatus
from typing import Any, List, Union

from crawler.types import FlaskResponse


def bad_request(errors: Union[str, List[str]]) -> FlaskResponse:
    if isinstance(errors, str):
        return {"errors": [errors]}, HTTPStatus.BAD_REQUEST

    return {"errors": errors}, HTTPStatus.BAD_REQUEST


def internal_server_error(errors: Union[str, List[str]], **kwargs: Any) -> FlaskResponse:
    if isinstance(errors, str):
        return {"errors": [errors], **kwargs}, HTTPStatus.INTERNAL_SERVER_ERROR

    return {"errors": errors, **kwargs}, HTTPStatus.INTERNAL_SERVER_ERROR


def ok(**kwargs: Any) -> FlaskResponse:
    return {**kwargs}, HTTPStatus.OK
