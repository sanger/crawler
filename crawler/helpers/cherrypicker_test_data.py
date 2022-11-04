import logging
import random
import uuid
from datetime import datetime

from crawler.constants import (
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_RNA_PCR_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_VIRAL_PREP_ID,
    TEST_DATA_CENTRE_LAB_ID,
)
from crawler.helpers.general_helpers import generate_baracoda_barcodes
from crawler.rabbit.messages.create_plate_message import CreatePlateMessage, Plate, Sample
from crawler.types import Config

LOGGER = logging.getLogger(__name__)


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


def create_barcodes(config: Config, num_required: int) -> list:
    # call Baracoda here and fetch a set of barcodes with the prefix we want
    LOGGER.info(f"Num barcodes required from Baracoda = {num_required}")
    list_barcodes = generate_baracoda_barcodes(config, BARACODA_PREFIX, num_required)
    return list_barcodes


def create_plate_messages(plate_specs: list, dt: datetime, list_barcodes: list) -> list:
    pos_per_plate = _flat_list_of_positives_per_plate(plate_specs)

    return [_create_plate_message(dt, list_barcodes[i], positives) for i, positives in enumerate(pos_per_plate)]


def create_barcode_meta(plate_specs: list, list_barcodes: list) -> list:
    LOGGER.info("Creating metadata for barcodes")

    pos_per_plate = _flat_list_of_positives_per_plate(plate_specs)
    return [[list_barcodes[i], f"number of positives: {pos_per_plate[i]}"] for i in range(len(pos_per_plate))]


def _flatten(nested_list: list) -> list:
    return [item for sublist in nested_list for item in sublist]


def _flat_list_of_positives_per_plate(plate_specs: list) -> list:
    # Turn [[2, 5], [3, 10]] into [5, 5, 10, 10, 10]
    return _flatten([[specs[1]] * specs[0] for specs in plate_specs])


def _create_root_sample_id(barcode: str, well_num: int) -> str:
    return "RSID-%s%s" % (barcode, str(well_num).zfill(2))


def _create_rna_id(barcode: str, well_coordinate: str) -> str:
    return "%s_%s" % (barcode, well_coordinate)


def _create_cog_uk_id(barcode: str, well_num: int) -> str:
    padded_hex_well_num = hex(well_num)[2:].zfill(2)
    return "%s%s" % (barcode, padded_hex_well_num)


def _create_sample(dt: datetime, index: int, result: str, plate_barcode: str) -> Sample:
    well_coordinate = WELL_COORDS[index]
    well_num = index + 1
    return Sample(
        sampleUuid=str(uuid.uuid4()).encode(),
        rootSampleId=_create_root_sample_id(plate_barcode, well_num),
        rnaId=_create_rna_id(plate_barcode, well_coordinate),
        cogUkId=_create_cog_uk_id(plate_barcode, well_num),
        plateCoordinate=well_coordinate,
        preferentiallySequence=False,
        mustSequence=False,
        fitToPick=True if result == "positive" else False,
        result=result,
        testedDateUtc=dt,
    )


def _create_samples(dt: datetime, plate_barcode: str, num_positives: int) -> list:
    num_negatives = PLATE_SIZE - num_positives
    results = ["positive"] * num_positives + ["negative"] * num_negatives
    random.shuffle(results)

    return [_create_sample(dt, index, result, plate_barcode) for index, result in enumerate(results)]


# TODO: At the moment, dt is used as a static timestamp across all samples and all plates It could be desirable to add
#       some random noise to this timestamp between either all samples or between plates.
def _create_plate_message(dt: datetime, plate_barcode: str, num_positives: int) -> CreatePlateMessage:
    samples = _create_samples(dt, plate_barcode, num_positives)
    plate = Plate(labId=TEST_DATA_CENTRE_LAB_ID, plateBarcode=plate_barcode, samples=samples)

    return CreatePlateMessage(messageUuid=str(uuid.uuid4()).encode(), messageCreateDateUtc=dt, plate=plate)
