import datetime
from functools import reduce
import logging


from crawler.helpers.cherrypicker_test_data import (
    create_barcodes,
    create_barcode_meta,
    create_csv_rows,
)

logger = logging.getLogger(__name__)


def generate(run_id: str) -> str:
    """Generates cherrypicker test data for processing by Crawler.

    The specification of the plates to be generated should be in Mongo.
    Each plate will contain an exact number of positive results between 0 and 96
    as specified. Up to 100 plates can be generated at a time.

    Arguments:
        run_id: str - The ID of the run.  If this is not found in Mongo an
            exception will be thrown.

    Returns:
        Metadata about the plates generated, as:
        [ [ "barcode1", "description1" ], [ "barcode2", "description2" ] ]
    """
    logger.info("Begin generating data.")

    # TODO: Get actual plate specs from Mongo
    plate_specs = [[1, 1], [2, 96]]
    num_plates = reduce(lambda a, b: a + b[0], plate_specs, 0)

    # TODO: Check the number of plates are 100 or fewer

    dt = datetime.datetime.now()
    barcodes = create_barcodes(num_plates)
    barcode_meta = create_barcode_meta(plate_specs, barcodes)
    csv_rows = create_csv_rows(plate_specs, dt, barcodes)
    # filename = write_file(dt, rows)

    return barcode_meta
