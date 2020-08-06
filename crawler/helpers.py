import logging
import os

# import pathlib
# import re
# import shutil
import sys

# from csv import DictReader, DictWriter
from datetime import datetime
from importlib import import_module
from types import ModuleType

# from typing import Any, Dict, List, Optional, Tuple
from typing import Tuple

import pysftp  # type: ignore

logger = logging.getLogger(__name__)


def current_time() -> str:
    """Generates a String containing a current timestamp in the format
    yymmdd_hhmm
    eg. 12:30 1st February 2019 becomes 190201_1230

    Returns:
        str -- A string with the current timestamp
    """
    return datetime.now().strftime("%y%m%d_%H%M")


def get_sftp_connection(
    config: ModuleType, username: str = None, password: str = None
) -> pysftp.Connection:
    """Get a connection to the SFTP server as a context manager. The READ credentials are used by
    default but a username and password provided will override these.

    Arguments:
        config {ModuleType} -- application config

    Keyword Arguments:
        username {str} -- username to use instead of the READ username (default: {None})
        password {str} -- password for the provided username (default: {None})

    Returns:
        pysftp.Connection -- a connection to the SFTP server as a context manager
    """
    # disable host key checking:
    #   https://bitbucket.org/dundeemt/pysftp/src/master/docs/cookbook.rst#rst-header-id5
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    sftp_host = config.SFTP_HOST  # type: ignore
    sftp_port = config.SFTP_PORT  # type: ignore
    sftp_username = config.SFTP_READ_USERNAME if username is None else username  # type: ignore
    sftp_password = config.SFTP_READ_PASSWORD if username is None else password  # type: ignore

    return pysftp.Connection(
        host=sftp_host,
        port=sftp_port,
        username=sftp_username,
        password=sftp_password,
        cnopts=cnopts,
    )


def get_config(settings_module: str) -> Tuple[ModuleType, str]:
    """Get the config for the app by importing a module named by an environmental variable. This
    allows easy switching between environments and inheriting default config values.

    Arguments:
        settings_module {str} -- the settings module to load

    Returns:
        Optional[ModuleType] -- the config module loaded and available to use via `config.<param>`
    """
    try:
        if not settings_module:
            settings_module = os.environ["SETTINGS_MODULE"]

        return import_module(settings_module), settings_module  # type: ignore
    except KeyError as e:
        sys.exit(f"{e} required in environmental variables for config")
