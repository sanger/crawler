from crawler.helpers import (
    get_config
)
from datetime import datetime
from migrations.helpers import mlwh_samples_update_helper
# import sys

# Start and End datetime parameters must be in format YYYY MM DD HH mm
def run(settings_module: str = "", s_start_datetime: str = "", s_end_datetime: str = "") -> None:
    config, settings_module = get_config(settings_module)

    print("-" * 80)
    print("STARTING LEGACY MLWH UPDATE")
    print(f"Time start: {datetime.now()}")
    print(f"Using settings from {settings_module}")

    mlwh_samples_update_helper.update_mlwh_with_legacy_samples(config, s_start_datetime, s_end_datetime)

    print(f"Time finished: {datetime.now()}")
    print("=" * 80)
