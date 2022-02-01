import pika
from time import sleep

def callback(ch, method, properties, body):
    if body:
        print(f"Doing something with the message: {body}.")
        sleep(1)
        print(f"I did something with the message: {body}.")
    else: 
        print("I'm not working :(")

if __name__ == "__main__":

    print("Starting to receive...")

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    queue = "sample-messenger-test"
    channel.queue_declare(queue = queue)

    channel.basic_consume(queue = queue, on_message_callback = callback, auto_ack = True)
    channel.start_consuming() # nom nom

    print("Finished consuming.")
