import pytest
import responses

from crawler.exceptions import TransientRabbitError
from crawler.rabbit.schema_registry import SchemaRegistry

BASE_URI = "http://schema_registry.com"
API_KEY = "EA7G00DF00D"


@pytest.fixture
def subject():
    subject = SchemaRegistry(BASE_URI, API_KEY)
    yield subject


def test_constructor_stores_values_correctly(subject):
    assert subject._base_uri == BASE_URI
    assert subject._api_key == API_KEY


@pytest.mark.parametrize(
    "schema_subject, schema_version",
    [
        ["test-subject-1", "1"],
        ["test-subject-2", "7"],
        ["test-subject-3", "latest"],
    ],
)
@responses.activate
def test_get_schema_generates_the_correct_request(subject, schema_subject, schema_version):
    expected_url = f"{BASE_URI}/subjects/{schema_subject}/versions/{schema_version}"

    responses.add(
        responses.GET,
        expected_url,
        json={},
        status=200,
    )

    subject.get_schema(schema_subject, schema_version)

    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == expected_url
    assert "X-API-KEY" in responses.calls[0].request.headers
    assert responses.calls[0].request.headers["X-API-KEY"] == API_KEY


@responses.activate
def test_get_schema_returns_the_response_json(subject):
    schema_subject = "create-plate-map"
    schema_version = "7"
    response_json = {"schema": "Some schema"}

    responses.add(
        responses.GET,
        f"{BASE_URI}/subjects/{schema_subject}/versions/{schema_version}",
        json=response_json,
        status=200,
    )

    result = subject.get_schema(schema_subject, schema_version)

    assert result == response_json


@responses.activate
def test_get_schema_without_a_version_gets_latest(subject):
    expected_url = f"{BASE_URI}/subjects/create-plate-map/versions/latest"

    responses.add(
        responses.GET,
        expected_url,
        json={},
        status=200,
    )

    subject.get_schema("create-plate-map")

    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == expected_url


@responses.activate
def test_get_schema_caches_responses(subject):
    schema_subject = "create-plate-map"
    schema_version = "7"
    response_json = {"schema": "Some schema"}

    responses.add(
        responses.GET,
        f"{BASE_URI}/subjects/{schema_subject}/versions/{schema_version}",
        json=response_json,
        status=200,
    )

    result1 = subject.get_schema(schema_subject, schema_version)
    assert result1 == response_json

    responses.reset()  # Stop responding to requests

    result2 = subject.get_schema(schema_subject, schema_version)
    assert result2 == response_json  # Note the result was the same after resetting the responses


def test_get_schema_raises_transient_rabbit_error_on_exception(subject):
    with pytest.raises(TransientRabbitError) as ex_info:
        subject.get_schema("no-schema-here", "42")

    assert BASE_URI in ex_info.value.message
