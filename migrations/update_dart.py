import time
from datetime import datetime

from crawler.types import Config
from migrations.helpers import dart_samples_update_helper


# Start and End datetime parameters must be in format YYMMDD_HHmm e.g. 200515_0900
def run(config: Config, s_start_datetime: str = "", s_end_datetime: str = "") -> None:
    print("-" * 80)
    print("STARTING LEGACY DART UPDATE")
    print(f"Time start: {datetime.now()}")
    start = time.time()

    dart_samples_update_helper.migrate_all_dbs(config, s_start_datetime, s_end_datetime)

    print(f"Time taken: {round(time.time() - start, 2)}s")
    print(f"Time finished: {datetime.now()}")
    print("=" * 80)
