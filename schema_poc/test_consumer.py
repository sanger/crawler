from os import getenv

from avro_rabbit_consumer import AvroRabbitConsumer
from credentials import CONSUME_CREDENTIALS, CREDENTIAL_KEY_API_KEY, CREDENTIAL_KEY_RABBITMQ
from hosts import RABBITMQ_HOST, REDPANDA_URL
from schema_registry import SchemaRegistry
from test_messages import QUEUES

# Before running this test, the schema is going to need to be loaded into RedPanda schema registry.
# The test here assume RedPanda is running on local host port 8081, which it will be if you used the dependencies
# Docker Compose file.  Then you need to have inserted the schema for this message type.  This can be done with PostMan
# by creating a POST request to http://localhost:8081/subjects/plate-map-sample/versions with the JSON body shown.
# Replace the schema definition with a slash escaped string based on the .avsc file in this directory.
# {
#     "schema": "slash_escaped_string_of_schema_json"
# }

subject = getenv("AVRO_TEST_SUBJECT", "create-plate-map")
queue = QUEUES[subject]
credentials = CONSUME_CREDENTIALS[subject]()

schema_registry = SchemaRegistry(REDPANDA_URL, credentials[CREDENTIAL_KEY_API_KEY])

# Read from RabbitMQ
consumer = AvroRabbitConsumer(RABBITMQ_HOST, 5671, schema_registry)
consumer.receive_messages(vhost="heron", queue=queue, username_password=credentials[CREDENTIAL_KEY_RABBITMQ])
