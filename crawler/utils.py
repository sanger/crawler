import os
from logging import Handler

from slack import WebClient  # type: ignore
from slack.errors import SlackApiError  # type: ignore

client = WebClient(token=os.getenv("SLACK_API_TOKEN", ""))


class SlackHandler(Handler):
    def emit(self, record):
        log_entry = self.format(record)
        self.send_message(log_entry)

    def send_message(self, sent_str):
        try:
            client.chat_postMessage(
                channel=os.getenv("SLACK_CHANNEL_ID", ""),
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Danny Torrence left the following review for your property:",
                        },
                    },
                    {"type": "section", "text": {"type": "mrkdwn", "text": sent_str}},
                ],
            )
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
