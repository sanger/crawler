from pika import BlockingConnection, ConnectionParameters
from schema_registry import SchemaRegistry, RESPONSE_KEY_VERSION, RESPONSE_KEY_SCHEMA, RESPONSE_KEY_SUBJECT
import json
from fastavro import parse_schema, json_reader
from io import StringIO

class Consumer:
    def __init__(self, schema_registry: SchemaRegistry):
        self._schema_registry = schema_registry

    def callback(self, ch, method, properties, body):
        if body:
            read_schema_response = self._schema_registry.get_schema(properties.headers[RESPONSE_KEY_SUBJECT],
                                                                    properties.headers[RESPONSE_KEY_VERSION])
            read_schema_obj = json.loads(read_schema_response[RESPONSE_KEY_SCHEMA])
            read_schema = parse_schema(read_schema_obj)
            string_reader = StringIO(body.decode('utf-8'))
            for sample in json_reader(string_reader, read_schema):
                print(sample)
        else:
            print("There was no body with the message - try again.")

    def receive_messages(self):
        connection = BlockingConnection(ConnectionParameters("localhost"))
        channel = connection.channel()
        queue = "sample-messenger"
        channel.queue_declare(queue=queue)
        channel.basic_consume(queue=queue, on_message_callback=self.callback, auto_ack=True)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

        connection.close()
        print("Finished consuming.")
