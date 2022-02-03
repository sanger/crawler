from datetime import datetime
from io import BytesIO

from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from avro.schema import parse as parse_schema


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

schema = parse_schema(open("plate_map_sample_v1.avsc", "rb").read())
writer = DatumWriter(schema)
bytes_writer = BytesIO()
encoder = BinaryEncoder(bytes_writer)
writer.write(sample1, encoder)
writer.write(sample1, encoder)

raw_bytes = bytes_writer.getvalue()

# Send to RabbitMQ at this point
# Let's now assume we just read raw_bytes from RabbitMQ

bytes_reader = BytesIO(raw_bytes)
decoder = BinaryDecoder(bytes_reader)
reader = DatumReader(schema)
try:
  while(True):
    sample = reader.read(decoder)
    sample["testedDateUtc"] = millis_to_datetime(sample["testedDateUtc"])
    sample["messageCreateDateUtc"] = millis_to_datetime(sample["messageCreateDateUtc"])
    print(sample)
except TypeError as ex:
  if "string of length 0 found" in str(ex):
    # This happens when we get to the end of the binary data
    pass
  else:
    raise
