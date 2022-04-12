import json
from io import StringIO

from fastavro import json_reader, json_writer, parse_schema

from crawler.rabbit.schema_registry import RESPONSE_KEY_SCHEMA, RESPONSE_KEY_VERSION


class AvroEncoder:
    def __init__(self, schema_registry, subject):
        self._schema_registry = schema_registry
        self._subject = subject

    def _schema_response(self, version):
        if version is None:
            return self._schema_registry.get_latest_schema(self._subject)
        else:
            return self._schema_registry.get_schema(self._subject, version)

    def _schema(self, schema_response):
        try:
            schema_obj = json.loads(schema_response[RESPONSE_KEY_SCHEMA])
        except KeyError as ex:
            raise ValueError("No valid schema returned from schema registry: %s", ex)

        return parse_schema(schema_obj)

    def _schema_version(self, schema_response):
        return schema_response[RESPONSE_KEY_VERSION]

    def encode(self, object, version=None):
        schema_response = self._schema_response(version)
        string_writer = StringIO()
        json_writer(string_writer, self._schema(schema_response), object)

        return (string_writer.getvalue(), self._schema_version(schema_response))

    def decode(self, message, version):
        schema_response = self._schema_response(version)
        string_reader = StringIO(message.decode("utf-8"))

        return json_reader(string_reader, self._schema(schema_response))
