class RabbitMessageProcessor:
    def __init__(self):
        self.config = None

    def process_message(self, headers, body, acknowledge):
        print(headers)
        print(body)
        print()

        acknowledge(True)
