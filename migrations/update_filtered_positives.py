from crawler.helpers import (
    get_config
)
from datetime import datetime
# from migrations.helpers import mlwh_samples_update_helper

def run(settings_module: str = "") -> None:
    config, settings_module = get_config(settings_module)

    print("-" * 80)
    print("STARTING FILTERED POSITIVES UPDATE")
    print(f"Time start: {datetime.now()}")

    # mlwh_samples_update_helper.update_mlwh_with_legacy_samples(config, s_start_datetime, s_end_datetime)

    print(f"Time finished: {datetime.now()}")
    print("=" * 80)
