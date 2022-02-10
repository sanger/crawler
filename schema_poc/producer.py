from pika import BlockingConnection, ConnectionParameters, BasicProperties

import json
from io import StringIO
from schema_registry import RESPONSE_KEY_SCHEMA, RESPONSE_KEY_VERSION, SchemaRegistry

from fastavro import json_writer, parse_schema

class Producer:
    def __init__(self, schema_registry: SchemaRegistry):
        self._schema_registry = schema_registry

    def prepare_message(self, message):
        subject = "plate-map-sample"
        write_schema_response = self._schema_registry.get_latest_schema(subject)
        write_schema_version = write_schema_response[RESPONSE_KEY_VERSION]
        write_schema_obj = json.loads(write_schema_response[RESPONSE_KEY_SCHEMA])

        write_schema = parse_schema(write_schema_obj)
        string_writer = StringIO()
        json_writer(string_writer, write_schema, message)

        prepared_message = {
            'subject': subject,
            'version': write_schema_version,
            'message': string_writer.getvalue()
        }

        return(prepared_message)

    def send_message(self, message_and_info):
        message = message_and_info["message"]
        version = message_and_info["version"]
        subject = message_and_info["subject"]

        connection = BlockingConnection(ConnectionParameters("localhost"))
        channel = connection.channel()
        queue = "sample-messenger"
        channel.queue_declare(queue=queue)
        channel.basic_publish(
            exchange="",
            routing_key=queue,
            properties = BasicProperties(headers = {"version": version, "subject": subject}),
            body=message)
        print(f"Sent the message.")
        connection.close()
