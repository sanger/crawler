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
        schedule.every().day.at("01:00").do(main.run)

        while True:
            schedule.run_pending()
            time.sleep(1)
