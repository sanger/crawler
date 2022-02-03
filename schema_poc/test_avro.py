from datetime import datetime

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from avro.schema import parse as parse_schema


def convert_to_millis(dt):
    return (int)(dt.timestamp() * 1000)


sample1 = {
    "labId": "CPTD",
    "sampleUuid": "UUID-123456-01",
    "plateBarcode": "BARCODE001",
    "plateCoordinate": "A6",
    "result": "positive",
    "preferentiallySequence": True,
    "mustSequence": True,
    "fitToPick": True,
    "testedDateUtc": convert_to_millis(datetime(2022, 2, 1, 13, 45, 8)),
    "messageCreateDateUtc": convert_to_millis(datetime.utcnow()),
}

schema = parse_schema(open("plate_map_sample_v1.avsc", "rb").read())

writer = DataFileWriter(open("plate_map_samples_v1.avro", "wb"), DatumWriter(), schema)
writer.append(sample1)
writer.close()

reader = DataFileReader(open("plate_map_samples_v1.avro", "rb"), DatumReader())
for sample in reader:
    print(sample)
reader.close()
