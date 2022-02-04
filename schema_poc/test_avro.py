import json
from datetime import datetime
from io import BytesIO

from fastavro import json_reader, json_writer, parse_schema


def datetime_to_millis(dt):
  return (int)(dt.timestamp() * 1000)

def millis_to_datetime(millis):
  return datetime.fromtimestamp(millis / 1000)


samples = [{
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
}]

with open("plate_map_sample_v1.avsc", "r") as schema_file:
  schema = parse_schema(json.load(schema_file))

with open("plate_map_samples_v1.avro", "w") as out_file:
  json_writer(out_file, schema, samples)

with open("plate_map_samples_v1.avro", "r") as in_file:
  for sample in json_reader(in_file, schema):
    print(sample)
