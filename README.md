# Crawler

![CI python](https://github.com/sanger/crawler/workflows/CI%20python/badge.svg?branch=develop)
![CI docker](https://github.com/sanger/crawler/workflows/CI%20docker/badge.svg?branch=develop)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![codecov](https://codecov.io/gh/sanger/crawler/branch/develop/graph/badge.svg)](https://codecov.io/gh/sanger/crawler)

This micro service saves sample information from external LIMS into a mongodb instance for easy
querying.

## Table of contents

<!-- toc -->

- [Requirements](#requirements)
- [Running](#running)
- [Migrations](#migrations)
  * [Updating the MLWH lighthouse_sample table](#updating-the-mlwh-lighthouse_sample-table)
  * [Filtered Positive Rules](#filtered-positive-rules)
    + [Version 0 `v0`](#version-0-v0)
    + [Version 1 `v1`](#version-1-v1)
    + [Version 2 `v2` - **Current Version**](#version-2-v2---current-version)
    + [Propagating Filtered Positive version changes to MongoDB, MLWH and (optional) DART](#propagating-filtered-positive-version-changes-to-mongodb-mlwh-and-optional-dart)
  * [Migrating legacy data to DART](#migrating-legacy-data-to-dart)
- [Priority Samples](#priority-samples)
- [Testing](#testing)
  * [Testing requirements](#testing-requirements)
  * [Running tests](#running-tests)
- [Formatting, type checking and linting](#formatting-type-checking-and-linting)
- [Miscellaneous](#miscellaneous)
  * [Naming conventions](#naming-conventions)
  * [Updating the table of contents](#updating-the-table-of-contents)

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

Once all the required packages are installed, enter the virtual environment with (this will also load the `.env` file):

    pipenv shell

The following runtime flags are available:

    python runner.py --help

    usage: runner.py [-h] [--sftp] [--scheduled]

    Store external samples in mongo.

    optional arguments:
    -h, --help    show this help message and exit
    --scheduled   start scheduled execution, defaults to running once
    --sftp        use SFTP to download CSV files, defaults to using local files
    --keep-files  keeps the CSV files after the runner has been executed
    --add-to-dart add samples to DART, by default they are not


## Migrations

### Updating the MLWH lighthouse_sample table

When the crawler process runs nightly it should be updating the MLWH lighthouse_sample table as it goes with records for
all rows that are inserted into MongoDB. If that MLWH insert process fails you should see a critical exception for the
file in Lighthouse-UI. This may be after records inserted correctly into MongoDB, and re-running the file will not
re-attempt the MLWH inserts in that situation.

There is a manual migration task that can be run to fix this discrepancy (update_mlwh_with_legacy_samples) that allows
insertion of rows to the MLWH between two MongoDB `created_at` datetimes.

__NB__: Both datetimes are inclusive: range includes those rows greater than or equal to start datetime, and less than
or equal to end datetime.

Usage (inside pipenv shell):

    python run_migration.py update_mlwh_with_legacy_samples 200115_1200 200116_1600

Where the time format is YYMMDD_HHmm. Both start and end timestamps must be present.

The process should not duplicate rows that are already present in MLWH, so you can be generous with your timestamp
range.

## Priority Samples

### Glossary:
- **Important**: refers to samples that have must_sequence or preferentially_sequence set to True
- **Positive**: refers to samples that have a _Positive_ Result
- **Pickable**: samples that have _Filtered Positive_ set to True **or** are Important in a plate with state 'pending'

If a sample is prioritised it will be treated the same as a fit_to_pick sample.

During the prioritisation run (after the file centres processing), any existing priority samples flagged as unprocessed will be:
   - Updated in MLWH lighthouse_sample with the values of the priority (must_sequence and preferentially_sequence) added to it
   - Inserted in DART as pickable if the plate is in state 'pending'
   - Updated as 'processed' in mongo so it won't be processed again unless there is a change for it

This will be applied with the following set of rules:
   - All records in mongodb from the priority_samples collection where **processed** is true will be ignored
   - All new updates of prioritisation will be updated in MLWH
   - If the sample is in a plate that does not have state 'pending' **no updates wiil be performed in DART for this sample**
   even if there is any new prioritisation set for it.
   - If the sample has filtered positive set to True, the sample will be flagged as **pickable** in DART
   - If the sample has any priority setting (must_sequence or preferentially_sequence), the sample will be flagged as **pickable** in DART
   - If the sample change its prioritisation to False, the setting for **pickable** will be removed in DART
   - After a record from priority_samples in mongodb has been processed, it will be flagged as **processed** set to True


### Filtered Positive Rules

This is a history of past and current rules by which positive samples are further filtered and identified as
'filtered positive'. Note that any rule change requires the `update_filtered_positives` migration be run, as outlined
in the below relevant section.

The implementation of the current version can be found in [FilteredPositiveIdentifier](./crawler/filtered_positive_identifier.py),
with the implementation of previous versions (if any) in the git history.

#### Version 0 `v0`

A sample is filtered positive if:

- it has a positive RESULT

This is the pre-"fit-to-pick" implementation, without any extra filtering on top of the RESULT=Positive requirement.

#### Version 1 `v1`

A sample is filtered positive if:

- it has a positive RESULT
- it is not a control (ROOT_SAMPLE_ID does not start with 'CBIQA_')
- all of CH1_CQ, CH2_CQ and CH3_CQ are `None`, or one of these is less than or equal to 30

More information on this version can be found on [this](https://ssg-confluence.internal.sanger.ac.uk/display/PSDPUB/UAT+6th+October+2020)
Confluence page.

#### Version 2 `v2` - **Current Version**

A sample is filtered positive if:

- it has a 'Positive' RESULT
- it is not a control (ROOT_SAMPLE_ID does not start with 'CBIQA_', 'QC0', or 'ZZA000')
- all of CH1_CQ, CH2_CQ and CH3_CQ are `None`, or one of these is less than or equal to 30

More information on this version can be found on [this](https://ssg-confluence.internal.sanger.ac.uk/display/PSDPUB/Fit+to+pick+-+v2)
Confluence page.

#### Propagating Filtered Positive version changes to MongoDB, MLWH and (optional) DART

On changing the positive filtering version/definition, all unpicked samples stored in MongoDB, MLWH and DART need
updating to determine whether they are still filtered positive under the new rules, and can therefore be cherrypicked.
In order to keep the databases in sync, the update process for all is performed in a single manual migration
(update_filtered_positives) which identifies unpicked samples, re-determines their filtered positive value, and updates
the databases.

Usage (inside pipenv shell):

    python run_migration.py update_filtered_positives

OR

    python run_migration.py update_filtered_positives omit_dart

By default, the migration will attempt to use DART, as it will safely fail if DART cannot be accessed, hence warning
the user to reconsider what they are doing. However, using DART can be omitted by including the `omit_dart` flag.
Neither process duplicates any data, instead updating existing entries.

### Migrating legacy data to DART

When the Beckman robots come online, we need to populate the DART database with the filtered positive samples that are
available physically. This can be achieved using the 'update_dart' migration.

This can also be used similarly to the existing MLWH migration: if a DART insert process fails, you will see a critical
exception for the file in the Lighthouse-UI. After addressing reason for failure, run between relevant timestamps to
re-insert/update data into DART.

In short, this migration performs the following steps:

1. Get the `RESULT = positive` samples (which are not controls) from mongo between a start and end date
1. Removes samples from this list which have already been cherrypicked by inspecting the events in the MLWH
1. Determining whether they are filtered positive samples using the latest rule
1. Determining the plate barcode UUID
1. Update mongo with the filtered positive and UUID values
1. Update MLWH with the same filtered positive and UUID values
1. Create/update the DART database with all the positive samples and setting the filtered positive samples as 'pickable'

To run the migration:

    python run_migration.py update_mlwh_and_dart_with_legacy_samples 200115_1200 200116_1600

Where the time format is YYMMDD_HHmm. Both start and end timestamps must be present.

## Testing

### Testing requirements

The tests require a connection to the 'lighthouse_sample' table in the Multi-LIMS Warehouse (MLWH). The credentials for
connecting to the MLWH are configured in the `defaults.py` file, or in the relevant environment file, for example
`test.py`. You can run the tests by connecting to the UAT instance of the MLWH, or an existing local copy you already
have. Or, you can create a basic local one containing just the relevant table by running the following from the top
level folder (this is what it does in the CI):

    python setup_test_db.py

### Running tests

To run the tests, execute:

    python -m pytest -vs

## Formatting, type checking and linting

Black is used as a formatter, to format code before commiting:

    black .

Mypy is used as a type checker, to execute:

    mypy .

Flake8 is used for linting, to execute:

    flake8

A little convenience script can be used to run the formatting, type checking and linting:

    ./forlint.sh

## Docker

If you do not have root access pyodbc will not work if you use brew
Using the docker compose you can set up the full stack and it will also set the correct environment variables

To build the containers:

    docker-compose up

To run the tests:

You will need to find the id of the container with image name crawler_runner

    docker exec -ti <container_id> python -m pytest -vs

There is now a volume for the runner so there is hot reloading i.e. changes in the code and tests will be updated when you rerun tests.

## Miscellaneous

### Mongo naming conventions

[This](https://stackoverflow.com/a/45335909) post was used for the naming conventions within mongo.

### MonkeyType

[MonkeyType](https://github.com/Instagram/MonkeyType) is useful to automatically create type annotations. View it's
README for updated instructions on how to use it.

### Updating the table of contents

Node is required to run npx:

    npx markdown-toc -i README.md
