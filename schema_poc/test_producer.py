from datetime import datetime
from os import getenv

from producer import Producer
from schema_registry import SchemaRegistry

# Before running this test, the schema is going to need to be loaded into RedPanda schema registry.
# The test here assume RedPanda is running on local host port 8081, which it will be if you used the dependencies
# Docker Compose file.  Then you need to have inserted the schema for this message type.  This can be done with PostMan
# by creating a POST request to http://localhost:8081/subjects/{subject_name}/versions with the JSON body shown.
# Replace {subject_name} in the URL with the intended subject name.  See the keys on MESSAGES below.
# Replace the schema definition with a slash escaped string based on the relevant .avsc file in this directory.
# {
#     "schema": "slash_escaped_string_of_schema_json"
# }

create_message = {
    "messageUuid": "UUID-789012-23",
    "messageCreateDateUtc": datetime.utcnow(),
    "operation": "create",
    "sample": {
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
    },
}

update_message = {
    "messageUuid": "UUID-789012-23",
    "messageCreateDateUtc": datetime.utcnow(),
    "sample": {
        "sampleUuid": "UUID-123456-01",
        "updatedFields": [
            {"name": "labId", "value": "CPTD"},
            {"name": "rootSampleId", "value": "R00T-S4MPL3-1D"},
            {"name": "plateBarcode", "value": "BARCODE001"},
            {"name": "plateCoordinate", "value": "A6"},
            {"name": "result", "value": "positive"},
            {"name": "cogUkId", "value": None},
            {"name": "preferentiallySequence", "value": True},
            {"name": "mustSequence", "value": True},
            {"name": "fitToPick", "value": True},
            {"name": "testedDateUtc", "value": datetime(2021, 3, 4, 10, 32, 48)},
        ],
    },
}

MESSAGES = {"plate-map-sample": [create_message], "update-plate-map-sample": [update_message]}

schema_registry = SchemaRegistry("http://localhost:8081")

producer = Producer(schema_registry)
subject = getenv("AVRO_TEST_SUBJECT", "plate-map-sample")
test_msg = MESSAGES[subject]
message_and_info = producer.prepare_message(test_msg, subject)
producer.send_message(message_and_info, exchange="", queue=subject)
