import json
import logging
import pathlib
from os import getenv
from typing import Dict

logger = logging.getLogger(__name__)


def get_config(test_db_config: Dict) -> Dict:
    configs = ("MONGO_HOST", "MONGO_PORT", "MONGO_DB", "CENTRE_DETAILS_FILE_PATH")

    try:
        # get config from environmental variables
        config = {conf: getenv(conf) for conf in configs}
    except Exception as e:
        logger.exception(e)
        raise Exception(f"The required configs are: {configs}")

    # overwrite config when testing
    if test_db_config:
        for conf in configs:
            if conf in test_db_config.keys():
                config[conf] = test_db_config[conf]

    logger.debug(f"Current config: {config}")

    return config


def get_centre_details(config: Dict) -> Dict:
    root = pathlib.Path(__file__).parent.parent
    centre_details_path = root.joinpath(f"{config['CENTRE_DETAILS_FILE_PATH']}")

    with open(centre_details_path) as centre_details_file:
        centre_details = json.loads(centre_details_file.read())

    return centre_details
