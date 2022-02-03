import pika

if __name__ == "__main__":

    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()

    queue = "sample-messenger-test"
    channel.queue_declare(queue=queue)

    message = "hello this is a message"
    channel.basic_publish(exchange="", routing_key=queue, body=message)

    print(f"Sent the message: '{message}'.")

    connection.close()
