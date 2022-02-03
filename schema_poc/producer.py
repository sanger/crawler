from pika import BlockingConnection, ConnectionParameters

if __name__ == "__main__":

    connection = BlockingConnection(ConnectionParameters("localhost"))
    channel = connection.channel()

    queue = "sample-messenger-test"
    channel.queue_declare(queue=queue)

    message = "hello this is a message"
    channel.basic_publish(exchange="", routing_key=queue, body=message.encode("utf-8"))

    print(f"Sent the message: '{message}'.")

    connection.close()
