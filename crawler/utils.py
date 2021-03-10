import os
import pprint
import sys
from logging import Filter, Handler, Logger
from typing import Iterable

from slack import WebClient
from slack.errors import SlackApiError


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


def pretty(logger: Logger, to_log: object) -> None:
    """Pretty prints the object to ease debugging. Logs at DEBUG and prints object over multiple calls to the logger
    so should not be used in production where exporting logs to logstash at per line.

    Arguments:
        logger {Logger} -- the logger to use
        to_log {object} -- object to be pretty printed
    """
    # https://stackoverflow.com/a/21024454
    for line in pprint.pformat(to_log).split("\n"):
        logger.debug(line)


class PackagePathFilter(Filter):
    """Subclass of logging Filter class which provides two log record helpers, namely:

    - relativepath: the relative path to the python module, this allows you to click on the path and line number from
    a terminal and open the source at the exact line in an IDE.
    - relative_path_and_lineno: a concatenation of `relativepath` and `lineno` to easily format the record helper to a
    certain length.

    Based heavily on https://stackoverflow.com/a/52582536/15200392
    """

    def filter(self, record):
        pathname = record.pathname

        record.relativepath = None
        record.relative_path_and_lineno = None

        abs_sys_paths: Iterable[str] = map(os.path.abspath, sys.path)

        for path in sorted(abs_sys_paths, key=len, reverse=True):  # longer paths first
            if not path.endswith(os.sep):
                path += os.sep
            if pathname.startswith(path):
                record.relativepath = os.path.relpath(pathname, path)
                record.relative_path_and_lineno = f"{record.relativepath}:{record.lineno}"

                break

        return True
