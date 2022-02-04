import json

from requests import get

RESPONSE_KEY_SCHEMA = "schema"


class SchemaRegistry:
    def __init__(self, base_uri: str):
        self._base_uri = base_uri

    def get_schema(self, subject: str, version_num: int) -> dict:
        response = get(f"{self._base_uri}/subjects/{subject}/versions/{version_num}")
        response_json = response.json()
        schema_string = response_json[RESPONSE_KEY_SCHEMA]
        return (dict)(json.loads(schema_string))
