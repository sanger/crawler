from datetime import datetime, timezone
from io import StringIO

from fastavro import json_reader, json_writer, parse_schema

SCHEMA = {
    "namespace": "uk.ac.sanger.psd",
    "type": "record",
    "name": "TestTimestamp",
    "fields": [{"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}],
}

# Get the UTC now timestamp
UTC_NOW = datetime.now(timezone.utc)

# Encode a basic message as JSON
schema = parse_schema(SCHEMA)
string_writer = StringIO()
json_writer(string_writer, schema, [{"timestamp": UTC_NOW}])
encoded_message = string_writer.getvalue()

string_reader = StringIO(encoded_message)
decoded_message = next(json_reader(string_reader, schema))

print(f"UTC now: {UTC_NOW}")
print(f"Encoded message: {encoded_message}")
print(f"Decoded message: {decoded_message!r}")

if type(decoded_message) == dict:
    print(f"Decoded timestamp: {decoded_message['timestamp']}")
