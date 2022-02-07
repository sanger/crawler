import json
from datetime import datetime
from io import StringIO

from fastavro import json_reader, json_writer, parse_schema
from schema_registry import RESPONSE_KEY_SCHEMA, RESPONSE_KEY_VERSION, SchemaRegistry

# Before running this test, the schema is going to need to be loaded into RedPanda schema registry.
# The test here assume RedPanda is running on local host port 8081, which it will be if you used the dependencies
# Docker Compose file.  Then you need to have inserted the schema for this message type.  This can be done with PostMan
# by creating a POST request to http://localhost:8081/subjects/plate-map-sample/versions with the JSON body shown.
# Replace the schema definition with a slash escaped string based on the .avsc file in this directory.
# {
#     "schema": "slash_escaped_string_of_schema_json"
# }

sample1 = {
    "labId": "CPTD",
    "sampleUuid": "UUID-123456-01",
    "plateBarcode": "BARCODE001",
    "rootSampleId": "R00T-S4MPL3-1D",
    "plateCoordinate": "A6",
    "result": "positive",
    "preferentiallySequence": True,
    "mustSequence": True,
    "fitToPick": True,
    "testedDateUtc": datetime(2022, 2, 1, 13, 45, 8),
    "messageCreateDateUtc": datetime.utcnow(),
    "messageUuid": "UUID-789012-23"
}

samples = [sample1, sample1]

schema_registry = SchemaRegistry("http://localhost:8081")

write_schema_response = schema_registry.get_latest_schema("plate-map-sample")
write_schema_obj = json.loads(write_schema_response[RESPONSE_KEY_SCHEMA])
write_schema = parse_schema(write_schema_obj)

string_writer = StringIO()
json_writer(string_writer, write_schema, samples)

message_json = string_writer.getvalue()

# Send to RabbitMQ at this point
# Let's now assume we just read raw_bytes from RabbitMQ

# Normally the schema version number would come from the properties on the RabbitMQ message
schema_version = write_schema_response[RESPONSE_KEY_VERSION]
read_schema_response = schema_registry.get_schema("plate-map-sample", schema_version)
read_schema_obj = json.loads(read_schema_response[RESPONSE_KEY_SCHEMA])
read_schema = parse_schema(read_schema_obj)

string_reader = StringIO(message_json)
for sample in json_reader(string_reader, read_schema):
    print(sample)
