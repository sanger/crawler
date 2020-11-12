# Report generation

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
