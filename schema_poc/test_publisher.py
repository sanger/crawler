from os import getenv

from avro_rabbit_publisher import AvroRabbitPublisher

# hosts.py is not in the default repository clone and will need creating
from schema_registry import SchemaRegistry
from test_messages import EXCHANGES, MESSAGES, ROUTING_KEYS

# Before running this test, the schema is going to need to be loaded into RedPanda schema registry.
# The test here assume RedPanda is running on local host port 8081, which it will be if you used the dependencies
# Docker Compose file.  Then you need to have inserted the schema for this message type.  This can be done with PostMan
# by creating a POST request to http://localhost:8081/subjects/{subject_name}/versions with the JSON body shown.
# Replace {subject_name} in the URL with the intended subject name.  See the keys on the MESSAGES object.
# Replace the schema definition with a slash escaped string based on the relevant .avsc file in this directory.
# {
#     "schema": "slash_escaped_string_of_schema_json"
# }


subject = getenv("AVRO_TEST_SUBJECT", "create-plate-map")
test_msg = MESSAGES[subject]
exchange = EXCHANGES[subject]
routing_key = ROUTING_KEYS[subject]

schema_registry = SchemaRegistry("http://localhost:8081")

producer = AvroRabbitPublisher("localhost", 5672, schema_registry)
prepared_message = producer.prepare_message(test_msg, subject)
producer.send_message(
    prepared_message,
    vhost="heron",
    exchange=exchange,
    routing_key=routing_key,
    username_password=("admin", "development"),
)
