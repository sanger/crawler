LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "colored": {
            "()": "colorlog.ColoredFormatter",
            "format": ("%(asctime)-15s %(name)-25s:%(lineno)-3s %(log_color)s%(levelname)-7s %(message)s"),
        },
        "verbose": {"format": "%(asctime)-15s %(name)-25s:%(lineno)-3s %(levelname)-7s %(message)s"},
    },
    "handlers": {
        "colored_stream": {
            "level": "DEBUG",
            "class": "colorlog.StreamHandler",
            "formatter": "colored",
        },
        "console": {"level": "INFO", "class": "logging.StreamHandler", "formatter": "verbose"},
        "slack": {
            "level": "ERROR",
            "class": "crawler.utils.SlackHandler",
            "formatter": "verbose",
            "token": "",
            "channel_id": "",
        },
    },
    "loggers": {
        "crawler": {"handlers": ["console", "slack"], "level": "INFO", "propagate": True},
        "migrations": {"handlers": ["colored_stream"], "level": "DEBUG", "propagate": True},
    },
}
