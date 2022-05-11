import json
from io import StringIO
from typing import Any, List, NamedTuple

import fastavro

from crawler.rabbit.schema_registry import RESPONSE_KEY_SCHEMA, RESPONSE_KEY_VERSION


class EncodedMessage(NamedTuple):
    body: bytes
    version: str


class AvroEncoder:
    def __init__(self, schema_registry, subject):
        self._schema_registry = schema_registry
        self._subject = subject

    def _schema_response(self, version):
        if version is None:
            return self._schema_registry.get_schema(self._subject)
        else:
            return self._schema_registry.get_schema(self._subject, version)

    def _schema(self, schema_response):
        schema_obj = json.loads(schema_response[RESPONSE_KEY_SCHEMA])
        return fastavro.parse_schema(schema_obj)

    def _schema_version(self, schema_response):
        return schema_response[RESPONSE_KEY_VERSION]

    def encode(self, records: List, version: str = None) -> EncodedMessage:
        schema_response = self._schema_response(version)
        string_writer = StringIO()
        fastavro.json_writer(string_writer, self._schema(schema_response), records)

        return EncodedMessage(
            body=string_writer.getvalue().encode(), version=str(self._schema_version(schema_response))
        )

    def decode(self, message: bytes, version: str) -> Any:
        schema_response = self._schema_response(version)
        string_reader = StringIO(message.decode("utf-8"))

        return fastavro.json_reader(string_reader, self._schema(schema_response))
