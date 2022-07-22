import logging
import random
import uuid
from datetime import datetime
from http import HTTPStatus

import requests

from crawler.constants import (
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_RNA_PCR_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_VIRAL_PREP_ID,
    TEST_DATA_CENTRE_LAB_ID,
    TEST_DATA_ERROR_BARACODA_COG_BARCODES,
    TEST_DATA_ERROR_BARACODA_CONNECTION,
    TEST_DATA_ERROR_BARACODA_UNKNOWN,
)
from crawler.exceptions import CherrypickerDataError
from crawler.rabbit.messages.create_plate_message import CreatePlateMessage, Plate, Sample
from crawler.types import Config

logger = logging.getLogger(__name__)


def make_coords_list():
    for column in "ABCDEFGH":
        for row in range(1, 13):
            yield f"{column}{row:02d}"


WELL_COORDS = list(make_coords_list())
PLATE_SIZE = len(WELL_COORDS)

SAMPLES_FILE_HEADERS = [
    FIELD_ROOT_SAMPLE_ID,
    FIELD_VIRAL_PREP_ID,
    FIELD_RNA_ID,
    FIELD_RNA_PCR_ID,
    FIELD_RESULT,
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
]

BARACODA_PREFIX = "TEST"


def flatten(nested_list: list) -> list:
    return [item for sublist in nested_list for item in sublist]


def generate_baracoda_barcodes(config: Config, num_required: int) -> list:
    baracoda_url = f"{config.BARACODA_BASE_URL}/barcodes_group/{BARACODA_PREFIX}/new?count={num_required}"

    retries = config.BARACODA_RETRY_ATTEMPTS
    except_obj = None
    response_json = None
    while retries > 0:
        try:
            response = requests.post(baracoda_url)
            if response.status_code == HTTPStatus.CREATED:
                response_json = response.json()
                barcodes: list = response_json["barcodes_group"]["barcodes"]
                return barcodes
            else:
                retries = retries - 1
                logger.error(TEST_DATA_ERROR_BARACODA_COG_BARCODES)
                logger.error(response.json())
                except_obj = CherrypickerDataError(TEST_DATA_ERROR_BARACODA_COG_BARCODES)
        except requests.ConnectionError as e:
            retries = retries - 1
            logger.error(TEST_DATA_ERROR_BARACODA_CONNECTION)
            except_obj = CherrypickerDataError(f"{TEST_DATA_ERROR_BARACODA_CONNECTION} -- {str(e)}")
        except Exception:
            retries = retries - 1
            logger.error(TEST_DATA_ERROR_BARACODA_UNKNOWN)

    if except_obj is not None:
        raise except_obj
    raise CherrypickerDataError(TEST_DATA_ERROR_BARACODA_UNKNOWN)


def create_barcodes(config: Config, num_required: int) -> list:
    # call Baracoda here and fetch a set of barcodes with the prefix we want
    logger.info(f"Num barcodes required from Baracoda = {num_required}")
    list_barcodes = generate_baracoda_barcodes(config, num_required)
    return list_barcodes


def create_root_sample_id(barcode: str, well_num: int) -> str:
    return "RSID-%s%s" % (barcode, str(well_num).zfill(2))


def create_rna_id(barcode: str, well_coordinate: str) -> str:
    return "%s_%s" % (barcode, well_coordinate)


def create_cog_uk_id(barcode: str, well_num: int) -> str:
    padded_hex_well_num = hex(well_num)[2:].zfill(2)
    return "%s%s" % (barcode, padded_hex_well_num)


def flat_list_of_positives_per_plate(plate_specs: list) -> list:
    # Turn [[2, 5], [3, 10]] into [5, 5, 10, 10, 10]
    return flatten([[specs[1]] * specs[0] for specs in plate_specs])


def create_sample(dt: datetime, index: int, result: str, plate_barcode: str) -> Sample:
    well_coordinate = WELL_COORDS[index]
    well_num = index + 1
    return Sample(
        sampleUuid=str(uuid.uuid4()).encode(),
        rootSampleId=create_root_sample_id(plate_barcode, well_num),
        rnaId=create_rna_id(plate_barcode, well_coordinate),
        cogUkId=create_cog_uk_id(plate_barcode, well_num),
        plateCoordinate=well_coordinate,
        preferentiallySequence=False,
        mustSequence=False,
        fitToPick=True,
        result=result,
        testedDateUtc=dt,
    )


def create_samples(dt: datetime, plate_barcode: str, num_positives: int) -> list:
    num_negatives = PLATE_SIZE - num_positives
    results = ["Positive"] * num_positives + ["Negative"] * num_negatives
    random.shuffle(results)

    return [create_sample(dt, index, result, plate_barcode) for index, result in enumerate(results)]


# TODO: At the moment, dt is used as a static timestamp across all samples and all plates It could be desirable to add
#       some random noise to this timestamp between either all samples or between plates.
def create_plate_message(dt: datetime, plate_barcode: str, num_positives: int) -> CreatePlateMessage:
    samples = create_samples(dt, plate_barcode, num_positives)
    plate = Plate(labId=TEST_DATA_CENTRE_LAB_ID, plateBarcode=plate_barcode, samples=samples)

    return CreatePlateMessage(messageUuid=str(uuid.uuid4()).encode(), messageCreateDateUtc=dt, plate=plate)


def create_plate_messages(plate_specs: list, dt: datetime, list_barcodes: list) -> list:
    pos_per_plate = flat_list_of_positives_per_plate(plate_specs)

    return [create_plate_message(dt, list_barcodes[i], positives) for i, positives in enumerate(pos_per_plate)]


def create_barcode_meta(plate_specs: list, list_barcodes: list) -> list:
    logger.info("Creating metadata for barcodes")

    pos_per_plate = flat_list_of_positives_per_plate(plate_specs)
    return [[list_barcodes[i], f"number of positives: {pos_per_plate[i]}"] for i in range(len(pos_per_plate))]
