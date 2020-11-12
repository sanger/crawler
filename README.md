# Crawler

![Docker CI](https://github.com/sanger/crawler/workflows/Docker%20CI/badge.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This micro service saves sample information from external LIMS into a mongodb instance for easy
querying.

<!-- toc -->

- [Requirements](#requirements)
- [Running](#running)
- [Updating the MLWH lighthouse_sample table](#updating-the-mlwh-lighthouse_sample-table)
- [Testing](#testing)
  - [Testing requirements](#testing-requirements)
  - [Running tests](#running-tests)
- [Mypy](#mypy)
- [Miscellaneous](#miscellaneous)
  - [Naming conventions](#naming-conventions)

<!-- tocstop -->

## Requirements

- python - install the required version specified in `Pipfile`:

        [requires]
        python_version = "<version>"

- install the required packages using [pipenv](https://github.com/pypa/pipenv):

        brew install pipenv
        pipenv install --dev

- Optionally, to test SFTP, [this](https://hub.docker.com/r/atmoz/sftp/) Docker image is helpful.

- mongodb

        brew tap mongodb/brew
        brew install mongodb-community@4.2
        brew services start mongodb-community@4.2

## Running

Once all the required packages are installed, enter the virtual environment with:

    pipenv shell

The following runtime flags are available:

    $ SETTINGS_MODULE=crawler.config.development python runner.py --help

    usage: runner.py [-h] [--sftp] [--scheduled]

    Store external samples in mongo.

    optional arguments:
    -h, --help   show this help message and exit
    --scheduled  start scheduled execution, defaults to running once
    --sftp       use SFTP to download CSV files, defaults to using local files
    --keep-files keeps centre csv files after runner has been executed

## Updating the MLWH lighthouse_sample table

When the crawler process runs nightly it should be updating the MLWH lighthouse_sample table as it goes with records for all rows that are inserted into MongoDB.
If that MLWH insert process fails you should see a critical exception for the file in Lighthouse-UI. This may be after records inserted correctly into MongoDB, and re-running the file will not re-attempt the MLWH inserts in that situation.
There is a manual migration task that can be run to fix this discrepancy (update_mlwh_with_legacy_samples) that allows insertion of rows to the MLWH between two MongoDB created_at datetimes.
NB. Both datetimes are inclusive: range includes those rows greater than or equal to start datetime, and less than or equal to end datetime.

Usage (inside pipenv shell):

    python run_migration.py update_mlwh_with_legacy_samples 200115_1200 200116_1600

Where the time format is YYMMDD_HHmm. Both start and end timestamps must be present.

The process should not duplicate rows that are already present in MLWH, so you can be generous with your timestamp range.

## Testing

### Testing requirements

The tests require a connection to the 'lighthouse_sample' table in the Multi-LIMS Warehouse (MLWH).
The credentials for connecting to the MLWH are configured in the `defaults.py` file, or in the
relevant environment file, for example `test.py`. You can run the tests by connecting to the UAT
instance of the MLWH, or an existing local copy you already have. Or, you can create a basic local
one containing just the relevant table by running the following from the top level folder (this is
what it does in the CI):

    python setup_test_db.py

### Running tests

To run the tests, execute:

    python -m pytest -vs

## Mypy

Mypy is used as a type checker, to execute:

    python -m mypy crawler

## Miscellaneous

### Naming conventions

[This](https://stackoverflow.com/a/45335909) post was used for the naming conventions within mongo.
