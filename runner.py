import argparse

from crawler import main
from crawler.config.centres import CENTRES
from crawler.constants import CENTRE_KEY_PREFIX

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse CSV files from the Lighthouse Labs and store the sample information in MongoDB"
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
        help="keeps the CSV files after the runner has been executed",
    )
    parser.add_argument(
        "--add-to-dart",
        dest="add_to_dart",
        action="store_true",
        help="on processing samples, also add them to DART",
    )
    parser.add_argument(
        "--centre-prefix",
        dest="centre_prefix",
        # Note that the choices are based on the local CENTRES config but filtering will be done based on
        # centres config in the MongoDB instance.
        choices=[centre[CENTRE_KEY_PREFIX] for centre in CENTRES],
        help="process only this centre's plate map files",
    )

    parser.set_defaults(sftp=False)
    parser.set_defaults(keep_files=False)
    parser.set_defaults(add_to_dart=False)

    args = parser.parse_args()

    main.run(sftp=args.sftp, keep_files=args.keep_files, add_to_dart=args.add_to_dart, centre_prefix=args.centre_prefix)
