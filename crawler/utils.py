from logging import Handler

from slack import WebClient  # type: ignore
from slack.errors import SlackApiError  # type: ignore


class SlackHandler(Handler):
    def __init__(self, token, channel_id):
        Handler.__init__(self)
        self.client = WebClient(token)
        self.channel_id = channel_id

    def emit(self, record):
        log_entry = self.format(record)
        self.send_message(log_entry)

    def send_message(self, sent_str):
        try:
            self.client.chat_postMessage(
                channel=self.channel_id,
                blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": sent_str}}],
            )
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
