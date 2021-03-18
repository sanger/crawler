from typing import Any, Dict

LOGGING: Dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "colored": {
            "style": "{",
            "()": "colorlog.ColoredFormatter",
            "format": "{asctime:<15} {name:<25}:{lineno:<3} {log_color}{levelname:<7} {message}",
        },
        "colored_dev": {
            "style": "{",
            "()": "colorlog.ColoredFormatter",
            "format": "{asctime:<15} {relative_path_and_lineno:<35} {log_color}{levelname:<7} {message}",
        },
        "verbose": {
            "style": "{",
            "format": "{asctime:<15} {name:<45}:{lineno:<3} {levelname:<7} {message}",
        },
    },
    "filters": {
        "package_path": {
            "()": "crawler.utils.PackagePathFilter",
        }
    },
    "handlers": {
        "colored_stream": {
            "level": "DEBUG",
            "class": "colorlog.StreamHandler",
            "formatter": "colored",
        },
        "colored_stream_dev": {
            "level": "DEBUG",
            "class": "colorlog.StreamHandler",
            "formatter": "colored_dev",
            "filters": ["package_path"],
        },
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "slack": {
            "level": "ERROR",
            "class": "crawler.utils.SlackHandler",
            "formatter": "verbose",
            "token": "",
            "channel_id": "",
        },
    },
    "loggers": {
        "crawler": {
            "handlers": ["console", "slack"],
            "level": "INFO",
            "propagate": True,
        },
        "migrations": {
            "handlers": ["colored_stream"],
            "level": "DEBUG",
            "propagate": True,
        },
    },
}
