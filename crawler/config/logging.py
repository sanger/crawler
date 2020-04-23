LOGGING_CONF = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "colored": {
            "()": "colorlog.ColoredFormatter",
            "format": "%(asctime)-15s %(name)-16s:%(lineno)s %(log_color)s%(levelname)-5s %(message)s",
        }
    },
    "handlers": {
        "colored_stream": {
            "level": "DEBUG",
            "class": "colorlog.StreamHandler",
            "formatter": "colored",
        },
    },
    "loggers": {"crawler": {"handlers": ["colored_stream"], "level": "DEBUG", "propagate": True,},},
}
