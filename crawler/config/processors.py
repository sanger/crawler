from typing import Dict, cast

from crawler.constants import RABBITMQ_SUBJECT_CREATE_PLATE, RABBITMQ_SUBJECT_UPDATE_SAMPLE
from lab_share_lib.processing.base_processor import BaseProcessor
from crawler.processing.create_plate_processor import CreatePlateProcessor
from crawler.processing.update_sample_processor import UpdateSampleProcessor

PROCESSORS: Dict[str, BaseProcessor] = {
    RABBITMQ_SUBJECT_CREATE_PLATE: cast(BaseProcessor, CreatePlateProcessor),
    RABBITMQ_SUBJECT_UPDATE_SAMPLE: cast(BaseProcessor, UpdateSampleProcessor),
}
