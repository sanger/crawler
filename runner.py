import argparse
import time
import logging
import logging.config

import schedule  # type: ignore

from crawler import main

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Store external samples in mongo.")

    parser.add_argument(
        "--scheduled",
        dest="once",
        action="store_false",
        help="start scheduled execution, defaults to running once",
    )
    parser.add_argument(
        "--sftp",
        dest="sftp",
        action="store_true",
        help="use SFTP to download CSV files, defaults to using local files",
    )
    parser.add_argument(
        "--keep-files",
        dest="keep_files",
        action="store_true",
        help="keeps centre csv files after runner has been executed",
    )
    parser.add_argument(
        "--add-to-dart",
        dest="add_to_dart",
        action="store_true",
        help="on processing samples, also add them to DART",
    )

    parser.set_defaults(once=True)
    parser.set_defaults(sftp=False)
    parser.set_defaults(keep_files=False)
    parser.set_defaults(add_to_dart=False)

    args = parser.parse_args()

    if args.once:
        main.run(args.sftp, args.keep_files, args.add_to_dart)
    else:
        print("Scheduled to run every 15 minutes")

        # if a run misses its scheduled time, it queues up
        # if more than one run is queued up, they execute sequentially
        # i.e. no parallel processing
        schedule.every(15).minutes.do(
            main.run, sftp=args.sftp, keep_files=args.keep_files, add_to_dart=args.add_to_dart
        )

        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error("There was an exception while running the scheduler")
                logger.exception(e)
                # We wait 60 seconds so it wont try to check it again during the same minute
                time.sleep(60)
