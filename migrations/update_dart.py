from datetime import datetime

from migrations.helpers import dart_samples_update_helper


# Start and End datetime parameters must be in format YYMMDD_HHmm e.g. 200515_0900
def run(config, s_start_datetime: str = "", s_end_datetime: str = "") -> None:
    print("-" * 80)
    print("STARTING LEGACY DART UPDATE")
    print(f"Time start: {datetime.now()}")

    dart_samples_update_helper.update_dart(config, s_start_datetime, s_end_datetime)

    print(f"Time finished: {datetime.now()}")
    print("=" * 80)
