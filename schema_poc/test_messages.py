from datetime import datetime, timezone

cherrypicked_message = {
    "samples": [
        {"sampleUuid": b"35d81426-2438-4a38-ae1b-97501d597362"},
        {"sampleUuid": b"e2f6484e-7133-4987-ae38-e0d65089f80b"},
        {"sampleUuid": b"35d81426-2438-4a38-ae1b-97501d597363"},
        {"sampleUuid": b"e2f6484e-7133-4987-ae38-e0d65089f80c"},
        {"sampleUuid": b"35d81426-2438-4a38-ae1b-97501d597364"},
        {"sampleUuid": b"e2f6484e-7133-4987-ae38-e0d65089f80d"},
    ]
}

create_message = {
    "messageUuid": b"b01aa0ad-7b19-4f94-87e9-70d74fb8783c",
    "messageCreateDateUtc": datetime.now(timezone.utc),
    "plate": {
        "labId": "CPTD",
        "plateBarcode": "BARCODE001",
        "samples": [
            {
                "sampleUuid": b"dd490ee5-fd1d-456d-99fd-eb9d3861e0f6",
                "rootSampleId": "R00T-S4MPL3-01",
                "cogUkId": "COG-UK-ID-1",
                "plateCoordinate": "A6",
                "rnaId": "BARCODE001_A06",
                "result": "positive",
                "preferentiallySequence": True,
                "mustSequence": True,
                "fitToPick": True,
                "testedDateUtc": datetime(2022, 2, 1, 13, 45, 8, tzinfo=timezone.utc),
            },
            {
                "sampleUuid": b"d1631fe4-6fd3-4f35-add1-8de2f54802c2",
                "rootSampleId": "R00T-S4MPL3-02",
                "cogUkId": "COG-UK-ID-2",
                "plateCoordinate": "B9",
                "rnaId": "BARCODE001_B09",
                "result": "negative",
                "preferentiallySequence": False,
                "mustSequence": False,
                "fitToPick": True,
                "testedDateUtc": datetime(2022, 2, 1, 13, 45, 14, tzinfo=timezone.utc),
            },
        ],
    },
}

create_feedback_message = {
    "sourceMessageUuid": "INVALID_UUID",
    "countOfTotalSamples": 94,
    "countOfValidSamples": 92,
    "operationWasErrorFree": False,
    "errors": [
        {
            "origin": "root",
            "sampleUuid": None,
            "field": "messageUuid",
            "description": "Provided UUID 'INVALID_UUID' is invalid.",
        },
        {
            "origin": "plate",
            "sampleUuid": None,
            "field": "labId",
            "description": "Value given 'undefined' is unrecognised.",
        },
        {
            "origin": "sample",
            "sampleUuid": "SAMPLE-UUID-002",
            "field": "plateCoordinate",
            "description": "Value given 'K15' is invalid.",
        },
        {
            "origin": "sample",
            "sampleUuid": "SAMPLE-UUID-008",
            "field": "testedDateUtc",
            "description": "Value given '2016-10-08' is not in the allowed range.",
        },
    ],
}

update_message = {
    "messageUuid": b"78fedc85-fa9d-494d-951e-779d208e8c0e",
    "messageCreateDateUtc": datetime.now(timezone.utc),
    "sample": {
        "sampleUuid": b"dd490ee5-fd1d-456d-99fd-eb9d3861e0f6",
        "updatedFields": [
            {"name": "preferentiallySequence", "value": False},
            {"name": "mustSequence", "value": False},
        ],
    },
}

update_feedback_message = {
    "sourceMessageUuid": "INVALID_UUID",
    "operationWasErrorFree": False,
    "errors": [
        {
            "origin": "root",
            "sampleUuid": None,
            "field": "messageUuid",
            "description": "Provided UUID 'INVALID_UUID' is invalid.",
        },
        {
            "origin": "field",
            "field": "plateCoordinate",
            "description": "Value given 'K15' is invalid.",
        },
        {
            "origin": "field",
            "field": "testedDateUtc",
            "description": "Value given '2016-10-08' is not in the allowed range.",
        },
    ],
}

MESSAGES = {
    "cherrypicked-samples": [cherrypicked_message],
    "create-plate-map": [create_message],
    "create-plate-map-feedback": [create_feedback_message],
    "update-plate-map-sample": [update_message],
    "update-plate-map-sample-feedback": [update_feedback_message],
}

EXCHANGES = {
    "cherrypicked-samples": "psd.heron",
    "create-plate-map": "pam.heron",
    "create-plate-map-feedback": "psd.heron",
    "update-plate-map-sample": "pam.heron",
    "update-plate-map-sample-feedback": "psd.heron",
}

ROUTING_KEYS = {
    "cherrypicked-samples": "feedback.cherrypicked.sample",
    "create-plate-map": "crud.create.plate",
    "create-plate-map-feedback": "feedback.created.plate",
    "update-plate-map-sample": "crud.update.sample",
    "update-plate-map-sample-feedback": "feedback.updated.sample",
}

QUEUES = {
    "cherrypicked-samples": "heron.feedback",
    "create-plate-map": "heron.crud-operations",
    "create-plate-map-feedback": "heron.feedback",
    "update-plate-map-sample": "heron.crud-operations",
    "update-plate-map-sample-feedback": "heron.feedback",
}
