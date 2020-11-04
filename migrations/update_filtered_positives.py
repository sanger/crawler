from crawler.helpers import (
    get_config
)
from datetime import datetime
from migrations.helpers import update_filtered_positives_helper

def run(settings_module: str = "") -> None:
    config, settings_module = get_config(settings_module)

    print("-" * 80)
    print("STARTING FILTERED POSITIVES UPDATE")
    print(f"Time start: {datetime.now()}")

    update_filtered_positives_helper.update_filtered_positives(config)

    print(f"Time finished: {datetime.now()}")
    print("=" * 80)
