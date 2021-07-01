import datetime
from functools import reduce
import logging


from crawler.helpers.cherrypicker_test_data import (
    create_barcodes,
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
        Metadata about the plates generated, as { "barcode": "description" }
    """
    logger.info("Begin generating data.")

    # TODO: Get actual plate specs from Mongo
    plate_specs = [[1, 1], [2, 96]]

    dt = datetime.datetime.now()
    num_plates = reduce(lambda a, b: a + b[0], plate_specs, 0)
    list_barcodes = create_barcodes(num_plates)
    csv_rows = create_csv_rows(plate_specs, dt, list_barcodes)
    # filename = write_file(dt, rows)
    # create_printing_file(list_barcodes, filename)

    return csv_rows
