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

    parser.set_defaults(once=True)
    parser.set_defaults(sftp=False)
    parser.set_defaults(keep_files=False)

    args = parser.parse_args()

    if args.once:
        main.run(args.sftp, args.keep_files)
    else:
        time_to_run = "1:00"
        print(f"Scheduled to run at {time_to_run}")
        schedule.every().day.at(time_to_run).do(
            main.run, sftp=args.sftp, keep_files=args.keep_files
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
