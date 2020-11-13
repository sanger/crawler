from crawler.db import create_mongo_client, get_mongo_db
from crawler.helpers import get_config

from migrations.helpers import sample_timestamps_helper


def run(settings_module: str = "") -> None:
    config, settings_module = get_config(settings_module)

    with create_mongo_client(config) as client:
        db = get_mongo_db(config, client)
        sample_timestamps_helper.add_timestamps_to_samples(db)
