import json
from io import StringIO

from constants import MESSAGE_PROPERTY_SUBJECT, MESSAGE_PROPERTY_VERSION
from fastavro import json_reader, parse_schema
from pika import BlockingConnection, ConnectionParameters, PlainCredentials, SSLOptions
from schema_registry import RESPONSE_KEY_SCHEMA, SchemaRegistry


class AvroRabbitConsumer:
    def __init__(self, host: str, port: int, schema_registry: SchemaRegistry):
        self._host = host
        self._port = port
        self._schema_registry = schema_registry

    def callback(self, ch, method, properties, body):
        if body:
            read_schema_response = self._schema_registry.get_schema(
                properties.headers[MESSAGE_PROPERTY_SUBJECT], properties.headers[MESSAGE_PROPERTY_VERSION]
            )
            read_schema_obj = json.loads(read_schema_response[RESPONSE_KEY_SCHEMA])
            read_schema = parse_schema(read_schema_obj)
            string_reader = StringIO(body.decode("utf-8"))
            for sample in json_reader(string_reader, read_schema):
                print(sample)
        else:
            print("There was no body with the message - try again.")

    def receive_messages(self, vhost, queue, username_password):
        credentials = PlainCredentials(username_password[0], username_password[1])
        connection_params = ConnectionParameters(
            host=self._host, port=self._port, virtual_host=vhost, credentials=credentials
        )
        connection = BlockingConnection(connection_params)
        channel = connection.channel()
        channel.basic_consume(queue=queue, on_message_callback=self.callback, auto_ack=True)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

        connection.close()
        print("Finished consuming.")
