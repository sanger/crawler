from datetime import datetime, timedelta
import uuid

from crawler.constants import (
    FIELD_CREATED_AT,
    FIELD_LAB_ID,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_UPDATED_AT,
)
from migrations.helpers.shared_helper import (
    extract_required_cp_info,
    remove_cherrypicked_samples,
)


# ----- test helpers -----


def generate_example_samples(range, start_datetime):
    samples = []
    #  create positive samples
    for n in range:
        samples.append(
            {
                FIELD_MONGODB_ID: str(uuid.uuid4()),
                FIELD_ROOT_SAMPLE_ID: f"TLS0000000{n}",
                FIELD_RESULT: "Positive",
                FIELD_PLATE_BARCODE: f"DN1000000{n}",
                FIELD_LAB_ID: "TLS",
                FIELD_RNA_ID: f"rna_{n}",
                FIELD_CREATED_AT: start_datetime + timedelta(days=n),
                FIELD_UPDATED_AT: start_datetime + timedelta(days=n),
            }
        )

    # create negative sample
    samples.append(
        {
            FIELD_MONGODB_ID: str(uuid.uuid4()),
            FIELD_ROOT_SAMPLE_ID: "TLS0000000_neg",
            FIELD_RESULT: "Negative",
            FIELD_PLATE_BARCODE: "DN10000000",
            FIELD_LAB_ID: "TLS",
            FIELD_RNA_ID: "rna_negative",
            FIELD_CREATED_AT: start_datetime,
            FIELD_UPDATED_AT: start_datetime,
        }
    )

    # create control sample
    samples.append(
        {
            FIELD_MONGODB_ID: str(uuid.uuid4()),
            FIELD_ROOT_SAMPLE_ID: "CBIQA_TLS0000000_control",
            FIELD_RESULT: "Positive",
            FIELD_PLATE_BARCODE: "DN10000000",
            FIELD_LAB_ID: "TLS",
            FIELD_RNA_ID: "rna_sample",
            FIELD_CREATED_AT: start_datetime,
            FIELD_UPDATED_AT: start_datetime,
        }
    )
    return samples


# ----- extract_required_cp_info tests -----


def test_extract_required_cp_info():
    test_samples = generate_example_samples(range(0, 3), datetime.now())
    test_samples.append(test_samples[0])

    expected_barcodes = set(["DN10000000", "DN10000001", "DN10000002"])
    expected_root_sample_ids = set(
        ["TLS00000000", "TLS00000001", "TLS00000002", "TLS0000000_neg", "CBIQA_TLS0000000_control"]
    )

    root_sample_ids, barcodes = extract_required_cp_info(test_samples)

    assert barcodes == expected_barcodes
    assert root_sample_ids == expected_root_sample_ids


# ----- remove_cherrypicked_samples tests -----


def test_remove_cherrypicked_samples():
    test_samples = generate_example_samples(range(0, 6), datetime.now())
    mock_cherry_picked_sample = [test_samples[0][FIELD_ROOT_SAMPLE_ID], test_samples[0][FIELD_PLATE_BARCODE]]

    samples = remove_cherrypicked_samples(test_samples, [mock_cherry_picked_sample])
    assert len(samples) == 7
    assert mock_cherry_picked_sample[0] not in [sample[FIELD_ROOT_SAMPLE_ID] for sample in samples]


# TODO - test get_cherrypicked_samples
