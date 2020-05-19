import argparse
import time

import schedule  # type: ignore

from crawler import main

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

    parser.set_defaults(once=True)
    parser.set_defaults(sftp=False)

    args = parser.parse_args()

    if args.once:
        main.run(args.sftp)
    else:
        time_to_run = "01:00"
        print(f"Scheduled to run at {time_to_run}")
        schedule.every().day.at(time_to_run).do(main.run, sftp=args.sftp)

        while True:
            schedule.run_pending()
            time.sleep(1)
