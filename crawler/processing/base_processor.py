from typing import Callable

from crawler.processing.rabbit_message import RabbitMessage


class BaseProcessor:
    process: Callable[["BaseProcessor", RabbitMessage], bool]
