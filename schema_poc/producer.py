import json
from io import StringIO

from constants import MESSAGE_PROPERTY_SUBJECT, MESSAGE_PROPERTY_VERSION
from fastavro import json_writer, parse_schema
from pika import BasicProperties, BlockingConnection, ConnectionParameters
from schema_registry import RESPONSE_KEY_SCHEMA, RESPONSE_KEY_VERSION, SchemaRegistry

MESSAGE_KEY_PROPERTIES = "properties"
MESSAGE_KEY_BODY = "body"


class Producer:
    def __init__(self, schema_registry: SchemaRegistry):
        self._schema_registry = schema_registry

    def prepare_message(self, message, subject):
        write_schema_response = self._schema_registry.get_latest_schema(subject)
        write_schema_obj = json.loads(write_schema_response[RESPONSE_KEY_SCHEMA])
        print(write_schema_obj)
        write_schema = parse_schema(write_schema_obj)
        string_writer = StringIO()
        json_writer(string_writer, write_schema, message)

        properties = BasicProperties(
            headers={
                MESSAGE_PROPERTY_SUBJECT: subject,
                MESSAGE_PROPERTY_VERSION: write_schema_response[RESPONSE_KEY_VERSION],
            }
        )

        return {MESSAGE_KEY_PROPERTIES: properties, MESSAGE_KEY_BODY: string_writer.getvalue()}

    def send_message(self, prepared_message, queue, exchange=""):
        connection = BlockingConnection(ConnectionParameters("localhost"))
        channel = connection.channel()
        channel.queue_declare(queue=queue)
        channel.basic_publish(
            exchange=exchange,
            routing_key=queue,
            properties=prepared_message[MESSAGE_KEY_PROPERTIES],
            body=prepared_message[MESSAGE_KEY_BODY],
        )
        print("Sent the message.")
        connection.close()
