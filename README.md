# Crawler

![Docker CI](https://github.com/sanger/crawler/workflows/Docker%20CI/badge.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This micro service saves sample information from external LIMS into a mongodb instance for easy
querying.

<!-- toc -->

- [Requirements](#requirements)
- [Running](#running)
- [Testing](#testing)
- [Mypy](#mypy)
- [Reporting](#reporting)
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

Optionally, to test SFTP, [this](https://hub.docker.com/r/atmoz/sftp/) Docker image is helpful.

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

    $ python run_migration.py update_mlwh_with_legacy_samples 200115_1200 200116_1600

Where the time format is YYMMDD_HHmm. Both start and end timestamps must be present.

The process should not duplicate rows that are already present in MLWH, so you can be generous with your timestamp range.

## Testing

The tests require a connection to the 'lighthouse_sample' table in the Multi-LIMS Warehouse.
The credentials for connecting to the MLWH are configured in the 'defaults.py' file, or in the relevant environment file, for example 'test.py'.
You can run the tests by connecting to the UAT instance of the MLWH, or an existing local copy you already have.
Or, you can create a basic local one containing just the relevant table by running the following from the top level folder (this is what it does in the CI):

    python setup_test_db.py

To run the tests, execute:

    python -m pytest -vs

## Mypy

Mypy is used as a type checker, to execute:

    python -m mypy crawler

## Reporting

To get a list of the positive samples which are on site:

Go to the Lighthouse UI app and click 'Create report'

To do it the old way:


1. Install `mongoexport` from the `mongodb-database-tools` bundle:

        brew install mongodb/brew/mongodb-database-tools

2. Enter the mongo shell from a terminal, substituting `<uri>` with a mongo uri that looks something
like `"mongodb://<user>:<password>@<host_address>/<database>"`:

        mongo "<uri>"

3. Verify that "Positive" is the only keyword that defines positive samples by executing the
following from a mongo shell:

        db.samples.distinct("Result")

4. Create a view (if it does not already exist) of the distinct plate barcodes which allows you to export the data to a CSV file:

        db.createView("ditinctPlateBarcode", "samples", [{ $match : { Result : "Positive" } } , { $group : { _id : "$plate_barcode" } }])

5. Export positive samples to a CSV file and select the fields required in the CSV:

        mongoexport --uri="<uri>" \
        --collection=samples \
        --out=samples.csv \
        --type=csv \
        --fields "source,plate_barcode,Root Sample ID,Result,Date Tested" \
        --query '{"Result":{ "$regularExpression": { "pattern": "^positive", "options": "i" }}}'

6. Export the plate barcodes to a CSV file:

        mongoexport --uri="<uri>" \
        --collection=ditinctPlateBarcode \
        --out=plate_barcodes.csv \
        --type=csv \
        --fields "_id"

7. Format the *plate_barcodes.csv* file by removing the first line (contains `_id`) and surrounding
each barcode with double quotation marks (`"`) and adding a comma (`,`) to the end of each line -
except the last:

        "<barcode 1>",
        "<barcode 2>",
        "<barcode 3>",
        ...
        "<barcode last>"

    This can be done using the following command (the first line still needs to be removed):

        cat plate_barcodes.csv | sed -e 's/\(.*\)/\"\1\"/g' | sed -e '$ ! s/$/,/g' > plates_barcodes_quoted.txt

8. Export a list (to CSV called *location_barcodes.csv*) of plate barcode to location barcode from
the labwhere database using the following query:

    ```sql
    SELECT
        labwares.barcode AS 'labwares.barcode',
        locations.barcode AS 'locations.barcode'
    FROM
        labwhere_production.labwares
            LEFT JOIN
        locations ON locations.id = labwares.location_id
    WHERE
        labwares.barcode IN (<plate barcode list from previous step>);
    ```

9. Open the CSV file created above (*location_barcodes.csv*) in Excel and create a filter on the
two columns and sort the `labwares.barcode` column in ascending order - save the file as `.xlsx`.
10. Open the *samples.csv* file created earlier and add a `VLOOKUP` formula to the first empty column
(name the column `location_barcode`) and drag down to copy the location barcode (`labwares.barcode`)
from the *location_barcodes.csv*
file:

        =VLOOKUP(B2,location_barcodes.xlsx!$A:$B,2,FALSE)

11. Add a filter to the entire dataset in *samples.csv* and filter the `location_barcode` column to
exclude those not having a match in *location_barcodes.csv*
12. Save *samples.csv* as *yyymmdd_hhmm_LH_onsite.xls*

## Miscellaneous

### Naming conventions

[This](https://stackoverflow.com/a/45335909) post was used for the naming conventions within mongo.
