import json
import logging
import os
import pathlib
from importlib import import_module
from os import getenv
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple

from crawler.exceptions import RequiredConfigError

logger = logging.getLogger(__name__)


def get_config(test_config: Dict[str, str] = None) -> Optional[ModuleType]:
    try:
        settings_module = os.environ["SETTINGS_MODULE"]

        logger.info(f"Using settings from {settings_module}")

        return import_module(settings_module)
    except KeyError as e:
        logger.error(f"{e} required in environmental variables for config")

        return None


# def get_config(test_config: Dict[str, str] = None) -> Dict[str, str]:
#     """Gets the config for the current application.

#     Arguments:
#         test_config {Dict[str, str]} -- config used while testing, overwrites default config

#     Raises:
#         RequiredConfigError: raised when required config is missing

#     Returns:
#         Dict[str, str] -- config to be used in app
#     """
#     logger.debug("Populating config dict")

#     configs: Tuple[str, ...] = (
#         "CENTRE_DETAILS_FILE_PATH",
#         "MONGO_DB",
#         "MONGO_HOST",
#         "MONGO_PASSWORD",
#         "MONGO_PORT",
#         "MONGO_USER",
#         "SFTP_HOST",
#         "SFTP_PASSWORD",
#         "SFTP_PORT",
#         "SFTP_USER",
#         "SLACK_API_TOKEN",
#         "SLACK_CHANNEL_ID",
#     )

#     config: Dict[str, Any] = {}
#     # if test config is None, get the config from the environmental variables
#     if test_config is None:
#         for conf in configs:
#             if (the_conf := getenv(conf)) is not None:
#                 config[conf] = the_conf
#             else:
#                 raise RequiredConfigError(conf)

#     # when testing, get the config from what is passed in and allow username and password to be
#     #   blank
#     if test_config:
#         for conf in configs:
#             if conf in test_config.keys():
#                 config[conf] = test_config[conf]
#             else:
#                 raise RequiredConfigError(conf)

#     return config


# def get_centre_details(config: Dict[str, str]) -> List[Dict[str, str]]:
#     """Get the cetre details from the JSON file.

#     Arguments:
#         config {Dict[str, str]} -- application config which specifies the centre details file path

#     Returns:
#         List[Dict[str, str]] -- the centre details
#     """
#     root = pathlib.Path(__file__).parent.parent
#     centre_details_path = root.joinpath(f"{config['CENTRE_DETAILS_FILE_PATH']}")

#     with open(centre_details_path) as centre_details_file:
#         centre_details = json.loads(centre_details_file.read())

#     return centre_details
