from datetime import datetime
from io import StringIO

from fastavro import json_reader, json_writer, parse_schema
from schema_registry import SchemaRegistry

sample1 = {
    "labId": "CPTD",
    "sampleUuid": "UUID-123456-01",
    "plateBarcode": "BARCODE001",
    "plateCoordinate": "A6",
    "result": "positive",
    "preferentiallySequence": True,
    "mustSequence": True,
    "fitToPick": True,
    "testedDateUtc": datetime(2022, 2, 1, 13, 45, 8),
    "messageCreateDateUtc": datetime.utcnow(),
}

samples = [sample1, sample1]

schema_registry = SchemaRegistry("http://localhost:8081")
schema_json = schema_registry.get_schema("plate-map-sample", 1)
schema = parse_schema(schema_json)

string_writer = StringIO()
json_writer(string_writer, schema, samples)

message_json = string_writer.getvalue()

# Send to RabbitMQ at this point
# Let's now assume we just read raw_bytes from RabbitMQ

string_reader = StringIO(message_json)
for sample in json_reader(string_reader, schema):
    print(sample)
