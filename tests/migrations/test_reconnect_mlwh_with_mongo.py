import pytest

from crawler.constants import FIELD_MONGODB_ID, FIELD_PLATE_BARCODE, FIELD_RNA_ID, MLWH_MONGODB_ID, MLWH_RNA_ID
from migrations import reconnect_mlwh_with_mongo


def test_reconnect_mlwh_with_mongo_missing_file(config):
    filepath = "not_found"
    with pytest.raises(Exception):
        reconnect_mlwh_with_mongo.run(config, filepath)


def test_reconnect_mlwh_with_mongo_not_raise_exception(
    config, testing_samples_with_lab_id, samples_collection_accessor, mlwh_testing_samples_unconnected
):
    filepath = "./tests/data/populate_old_plates.csv"
    try:
        reconnect_mlwh_with_mongo.run(config, filepath)
    except Exception as exc:
        raise AssertionError(exc)


def test_reconnect_mlwh_with_mongo_can_connect_with_mlwh(
    config,
    query_lighthouse_sample,
    testing_samples_with_lab_id,
    samples_collection_accessor,
    mlwh_testing_samples_unconnected,
):
    filepath = "./tests/data/populate_old_plates.csv"

    reconnect_mlwh_with_mongo.run(config, filepath)

    # Now we check in mlwh
    cursor = query_lighthouse_sample.execute(
        "SELECT rna_id, mongodb_id FROM lighthouse_sample WHERE plate_barcode = '123'"
    )

    obtained_mlwh_samples = list(cursor.fetchall())
    assert obtained_mlwh_samples[0]["mongodb_id"] == str(testing_samples_with_lab_id[0][FIELD_MONGODB_ID])

    mlwh_dict = {}
    for mlsample in obtained_mlwh_samples:
        mlwh_dict[mlsample[MLWH_RNA_ID]] = mlsample[MLWH_MONGODB_ID]

    mongo_dict = {}
    for sample in testing_samples_with_lab_id:
        if sample[FIELD_PLATE_BARCODE] == "123":
            mongo_dict[sample[FIELD_RNA_ID]] = str(sample[FIELD_MONGODB_ID])

    for rna_id in mongo_dict.keys():
        assert mongo_dict[rna_id] == mlwh_dict[rna_id]
