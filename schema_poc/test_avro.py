import json
from datetime import datetime
from io import StringIO

from fastavro import json_reader, json_writer, parse_schema


def datetime_to_millis(dt):
    return (int)(dt.timestamp() * 1000)


def millis_to_datetime(millis):
    return datetime.fromtimestamp(millis / 1000)


sample1 = {
    "labId": "CPTD",
    "sampleUuid": "UUID-123456-01",
    "plateBarcode": "BARCODE001",
    "plateCoordinate": "A6",
    "result": "positive",
    "preferentiallySequence": True,
    "mustSequence": True,
    "fitToPick": True,
    "testedDateUtc": datetime_to_millis(datetime(2022, 2, 1, 13, 45, 8)),
    "messageCreateDateUtc": datetime_to_millis(datetime.utcnow()),
}

samples = [sample1, sample1]

with open("plate_map_sample_v1.avsc", "r") as schema_file:
    schema = parse_schema(json.load(schema_file))

string_writer = StringIO()
json_writer(string_writer, schema, samples)

message_json = string_writer.getvalue()

# Send to RabbitMQ at this point
# Let's now assume we just read raw_bytes from RabbitMQ

string_reader = StringIO(message_json)
for sample in json_reader(string_reader, schema):
    print(sample)
