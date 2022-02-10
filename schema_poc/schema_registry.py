from typing import Union

from requests import get

RESPONSE_KEY_VERSION = "version"
RESPONSE_KEY_SCHEMA = "schema"
RESPONSE_KEY_SUBJECT = "subject"

class SchemaRegistry:
    def __init__(self, base_uri: str):
        self._base_uri = base_uri

    def get_schema(self, subject: str, version_num: Union[str, int]) -> dict:
        return (dict)(get(f"{self._base_uri}/subjects/{subject}/versions/{version_num}").json())

    def get_latest_schema(self, subject: str) -> dict:
        return self.get_schema(subject, "latest")
