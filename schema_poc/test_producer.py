from datetime import datetime
from schema_registry import SchemaRegistry
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
    "testedDateUtc": datetime(2022, 2, 1, 13, 45, 8),
}

test_sample_message = [
    {
        "messageUuid": "UUID-789012-23",
        "messageCreateDateUtc": datetime.utcnow(),
        "operation": "create",
        "sample": sample,
    }
]

test_feedback_message = [
    {
        "messageUuid": "UUID-789012-23",
        "operation": "createPlateFeedback",
        "operationSuccessful": False,
        "feedback": {
            "plateErrors": {
                    "errorMessage": "ERROR MESSAGE: SOMETHING HAPPENED"
            },
            "successCount": 1,
            "failureCount": 2,
            "samples": [
                {
                    "sampleUuid": "id1",
                    "sampleSuccessfullyProcessed": True,
                    "sampleErrors": {
                        "200"
                    }
                },
                {
                    "sampleUuid": "id2",
                    "sampleSuccessfullyProcessed": True,
                    "sampleErrors": {
                        "404: SAMPLE NOT FOUND."
                    }
                },
                {
                    "sampleUuid": "id1",
                    "sampleSuccessfullyProcessed": True,
                    "sampleErrors": {
                        "403: CAN'T DO THAT."
                    }
                }
            ]
        }
    }
]



schema_registry = SchemaRegistry("http://localhost:8081")

producer = Producer(schema_registry)
subject = "plate-map-sample-feedback"
message_and_info = producer.prepare_message(test_feedback_message, subject)
producer.send_message(message_and_info, exchange="", queue="sample-messenger")
