import logging


logger = logging.getLogger(__name__)


def generate(run_id: str) -> str:
    """Generates cherrypicker test data for processing by Crawler.

    The specification of the plates to be generated should be in Mongo.
    Each plate will contain an exact number of positive results between 0 and 96
    as specified. Up to 100 plates can be generated at a time.

    Arguments:
        run_id: str - The ID of the run.  If this is not found in Mongo an
            exception will be thrown.
    """
    logger.info("Begin generating data.")

    return { "TP-012345": "Plate with 96 positives" }
