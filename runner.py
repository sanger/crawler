import argparse
import time

import schedule

from crawler import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Store external samples in mongo.")

    # TODO: add option to skip sftp
    parser.add_argument("--run-once", dest="once", action="store_true")
    parser.set_defaults(once=False)

    args = parser.parse_args()

    if args.once:
        main.run()
    else:
        schedule.every().day.at("01:00").do(main.run)

        while True:
            schedule.run_pending()
            time.sleep(1)
