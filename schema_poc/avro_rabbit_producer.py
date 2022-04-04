import json
from io import StringIO
from ssl import create_default_context

from constants import MESSAGE_PROPERTY_SUBJECT, MESSAGE_PROPERTY_VERSION
from fastavro import json_writer, parse_schema
from pika import BasicProperties, BlockingConnection, ConnectionParameters, PlainCredentials, SSLOptions
from schema_registry import RESPONSE_KEY_SCHEMA, RESPONSE_KEY_VERSION, SchemaRegistry

MESSAGE_KEY_PROPERTIES = "properties"
MESSAGE_KEY_BODY = "body"


class AvroRabbitProducer:
    def __init__(self, host: str, port: int, schema_registry: SchemaRegistry):
        self._host = host
        self._port = port
        self._schema_registry = schema_registry

    def prepare_message(self, message, subject):
        write_schema_response = self._schema_registry.get_latest_schema(subject)
        write_schema_obj = json.loads(write_schema_response[RESPONSE_KEY_SCHEMA])
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

    def send_message(self, prepared_message, vhost, exchange, routing_key, username_password):
        credentials = PlainCredentials(username_password[0], username_password[1])
        ssl_options = SSLOptions(create_default_context())
        connection_params = ConnectionParameters(
            host=self._host, port=self._port, virtual_host=vhost, credentials=credentials, ssl_options=ssl_options
        )
        connection = BlockingConnection(connection_params)
        channel = connection.channel()
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            properties=prepared_message[MESSAGE_KEY_PROPERTIES],
            body=prepared_message[MESSAGE_KEY_BODY],
        )
        print("Sent the message.")
        connection.close()
