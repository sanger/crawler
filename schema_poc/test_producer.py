from datetime import datetime
from schema_registry import RESPONSE_KEY_SCHEMA, RESPONSE_KEY_VERSION, SchemaRegistry
from producer import Producer

# Before running this test, the schema is going to need to be loaded into RedPanda schema registry.
# The test here assume RedPanda is running on local host port 8081, which it will be if you used the dependencies
# Docker Compose file.  Then you need to have inserted the schema for this message type.  This can be done with PostMan
# by creating a POST request to http://localhost:8081/subjects/plate-map-sample/versions with the JSON body shown.
# Replace the schema definition with a slash escaped string based on the .avsc file in this directory.
# {
#     "schema": "slash_escaped_string_of_schema_json"
# }

sample = {
    "labId": "CPTD",
    "sampleUuid": "UUID-123456-01",
    "plateBarcode": "BARCODE001",
    "rootSampleId": "R00T-S4MPL3-1D",
    "plateCoordinate": "A6",
    "result": "positive",
    "preferentiallySequence": True,
    "mustSequence": True,
    "fitToPick": True,
    "testedDateUtc": datetime(2022, 2, 1, 13, 45, 8)
}

test_msg = [{
    "messageUuid": "UUID-789012-23",
    "messageCreateDateUtc": datetime.utcnow(),
    "operation": "create",
    "sample": sample
}]

schema_registry = SchemaRegistry("http://localhost:8081")

producer = Producer(schema_registry)
message_and_info = producer.prepare_message(test_msg)
producer.send_message(message_and_info, exchange="", queue = "sample-messenger")
