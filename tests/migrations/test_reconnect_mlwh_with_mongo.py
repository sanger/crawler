import pytest

from crawler.constants import FIELD_MONGODB_ID, FIELD_PLATE_BARCODE, FIELD_RNA_ID, MLWH_MONGODB_ID, MLWH_RNA_ID
from migrations import reconnect_mlwh_with_mongo
from tests.testing_objects import TESTING_SAMPLES_WITH_LAB_ID


def test_reconnect_mlwh_with_mongo_missing_file(config):
    filepath = "not_found"
    with pytest.raises(FileNotFoundError):
        reconnect_mlwh_with_mongo.run(config, filepath)


@pytest.mark.parametrize("samples_collection_accessor", [TESTING_SAMPLES_WITH_LAB_ID], indirect=True)
def test_reconnect_mlwh_with_mongo_not_raise_exception(
    config, samples_collection_accessor, mlwh_testing_samples_unconnected
):
    filepath = "./tests/data/populate_old_plates_1.csv"
    try:
        reconnect_mlwh_with_mongo.run(config, filepath)
    except Exception as exc:
        raise AssertionError(exc)


@pytest.mark.parametrize("samples_collection_accessor", [TESTING_SAMPLES_WITH_LAB_ID], indirect=True)
def test_reconnect_mlwh_with_mongo_can_connect_with_mlwh(
    config,
    query_lighthouse_sample,
    samples_collection_accessor,
    mlwh_testing_samples_unconnected,
):
    filepath = "./tests/data/populate_old_plates_1.csv"
    samples_in_mongo = list(samples_collection_accessor.find({}))

    reconnect_mlwh_with_mongo.run(config, filepath)

    # Now we check in mlwh
    cursor = query_lighthouse_sample.execute(
        "SELECT rna_id, mongodb_id FROM lighthouse_sample WHERE plate_barcode = '123'"
    )

    obtained_mlwh_samples = list(cursor.fetchall())
    assert obtained_mlwh_samples[0]["mongodb_id"] == str(samples_in_mongo[0][FIELD_MONGODB_ID])

    mlwh_dict = {}
    for mlsample in obtained_mlwh_samples:
        mlwh_dict[mlsample[MLWH_RNA_ID]] = mlsample[MLWH_MONGODB_ID]

    mongo_dict = {}
    for sample in samples_in_mongo:
        if sample[FIELD_PLATE_BARCODE] == "123":
            mongo_dict[sample[FIELD_RNA_ID]] = str(sample[FIELD_MONGODB_ID])

    for rna_id in mongo_dict.keys():
        assert mongo_dict[rna_id] == mlwh_dict[rna_id]
