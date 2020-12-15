from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from crawler.constants import (
    DART_STATE_PENDING,
    FIELD_COORDINATE,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_MONGODB_ID,
    FIELD_PLATE_BARCODE,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SOURCE,
    MLWH_COORDINATE,
    MLWH_FILTERED_POSITIVE,
    MLWH_FILTERED_POSITIVE_TIMESTAMP,
    MLWH_FILTERED_POSITIVE_VERSION,
    MLWH_MONGODB_ID,
    MLWH_PLATE_BARCODE,
    MLWH_RESULT,
    MLWH_RNA_ID,
    MLWH_ROOT_SAMPLE_ID,
    POSITIVE_RESULT_VALUE,
)
from crawler.sql_queries import SQL_DART_GET_PLATE_BARCODES
from migrations.helpers.update_filtered_positives_helper import (
    biomek_labclass_by_centre_name,
    remove_cherrypicked_samples,
    pending_plate_barcodes_from_dart,
    positive_result_samples_from_mongo,
    update_dart_fields,
    update_filtered_positive_fields,
    update_mlwh_filtered_positive_fields,
    update_mongo_filtered_positive_fields,
)

# ----- test fixture helpers -----


@pytest.fixture
def mock_dart_conn():
    with patch("migrations.helpers.update_filtered_positives_helper.create_dart_sql_server_conn") as mock_connect:
        yield mock_connect


@pytest.fixture
def mock_mongo_client():
    with patch("migrations.helpers.update_filtered_positives_helper.create_mongo_client") as mock_client:
        yield mock_client


@pytest.fixture
def mock_mongo_collection():
    with patch("migrations.helpers.update_filtered_positives_helper.get_mongo_collection") as mock_collection:
        yield mock_collection


# ----- test pending_plate_barcodes_from_dart method -----


def test_pending_plate_barcodes_from_dart_throws_for_error_generating_connection(config, mock_dart_conn):
    mock_dart_conn.side_effect = Exception("Boom!")
    with pytest.raises(Exception):
        pending_plate_barcodes_from_dart(config)


def test_pending_plate_barcodes_from_dart_throws_for_no_connection(config, mock_dart_conn):
    mock_dart_conn.return_value = None
    with pytest.raises(ValueError):
        pending_plate_barcodes_from_dart(config)


def test_pending_plate_barcodes_from_dart_throws_for_error_generating_cursor(config, mock_dart_conn):
    mock_dart_conn().cursor.side_effect = NotImplementedError("Boom!")
    with pytest.raises(NotImplementedError):
        pending_plate_barcodes_from_dart(config)


def test_pending_plate_barcodes_from_dart_handles_error_executing_statement(config, mock_dart_conn):
    mock_dart_conn().cursor().execute.side_effect = Exception("Boom!")
    pending_plate_barcodes_from_dart(config)


def test_pending_plate_barcodes_from_dart_returns_expected_plate_barcodes(config, mock_dart_conn):
    expected_rows = [("ABC123",), ("123ABC",), ("abcdef",)]
    mock_dart_conn().cursor().execute().fetchall.return_value = expected_rows
    result = pending_plate_barcodes_from_dart(config)

    mock_dart_conn().cursor().execute.assert_called_with(SQL_DART_GET_PLATE_BARCODES, DART_STATE_PENDING)
    assert result == ["ABC123", "123ABC", "abcdef"]


# ----- test positive_result_samples_from_mongo method -----


def test_positive_result_samples_from_mongo_throws_for_errors_creating_client(config, mock_mongo_client):
    mock_mongo_client.side_effect = Exception("Boom!")
    with pytest.raises(Exception):
        positive_result_samples_from_mongo(config, [])


def test_positive_result_samples_from_mongo_throws_for_error_creating_db(config):
    with patch("migrations.helpers.update_filtered_positives_helper.get_mongo_db") as mongo_db:
        mongo_db.side_effect = NotImplementedError("Boom!")
        with pytest.raises(NotImplementedError):
            positive_result_samples_from_mongo(config, [])


def test_positive_result_samples_from_mongo_throws_for_error_getting_collection(config, mock_mongo_collection):
    mock_mongo_collection.side_effect = ValueError("Boom!")
    with pytest.raises(ValueError):
        positive_result_samples_from_mongo(config, [])


def test_positive_result_samples_from_mongo_throws_for_error_finding_samples(config, mock_mongo_collection):
    mock_mongo_collection().find.side_effect = Exception("Boom!")
    with pytest.raises(Exception):
        positive_result_samples_from_mongo(config, [])


def test_positive_result_samples_from_mongo_returns_expected_samples(config, testing_samples):
    plate_barcodes = ["123"]
    expected_samples = testing_samples[:1]  # only the first sample is positive, with matching plate barcode
    result = positive_result_samples_from_mongo(config, plate_barcodes)
    assert result == expected_samples


# ----- test remove_cherrypicked_samples method -----


def test_remove_cherrypicked_samples_throws_for_error_extracting_required_cp_info(config, testing_samples):
    with patch(
        "migrations.helpers.update_filtered_positives_helper.extract_required_cp_info", side_effect=Exception("Boom!")
    ):
        with pytest.raises(Exception):
            remove_cherrypicked_samples(config, testing_samples)


def test_remove_cherrypicked_samples_throws_for_error_getting_cherrypicked_samples(config, testing_samples):
    with patch(
        "migrations.helpers.update_filtered_positives_helper.get_cherrypicked_samples", side_effect=Exception("Boom!")
    ):
        with pytest.raises(Exception):
            remove_cherrypicked_samples(config, testing_samples)


def test_remove_cherrypicked_samples_returns_no_samples_with_none_cp_samples_df(config, testing_samples):
    with patch("migrations.helpers.update_filtered_positives_helper.get_cherrypicked_samples", return_value=None):
        result = remove_cherrypicked_samples(config, testing_samples)
        assert result == []


def test_remove_cherrypicked_samples_returns_no_samples_with_empty_cp_samples_df(config, testing_samples):
    cp_samples_df = MagicMock()
    type(cp_samples_df).empty = PropertyMock(return_value=True)
    with patch(
        "migrations.helpers.update_filtered_positives_helper.get_cherrypicked_samples", return_value=cp_samples_df
    ):
        result = remove_cherrypicked_samples(config, testing_samples)
        assert result == []


def test_remove_cherrypicked_samples_throws_for_error_removing_cp_samples(config, testing_samples):
    cp_samples_df = MagicMock()
    type(cp_samples_df).empty = PropertyMock(return_value=False)
    with patch(
        "migrations.helpers.update_filtered_positives_helper.get_cherrypicked_samples", return_value=cp_samples_df
    ):
        with patch(
            "migrations.helpers.update_filtered_positives_helper.remove_cp_samples", side_effect=Exception("Boom!")
        ):
            with pytest.raises(Exception):
                remove_cherrypicked_samples(config, testing_samples)


def test_remove_cherrypicked_samples_returns_non_cp_samples(config, testing_samples):
    cp_samples_df = MagicMock()
    type(cp_samples_df).empty = PropertyMock(return_value=False)
    with patch(
        "migrations.helpers.update_filtered_positives_helper.get_cherrypicked_samples", return_value=cp_samples_df
    ):
        with patch(
            "migrations.helpers.update_filtered_positives_helper.remove_cp_samples", return_value=testing_samples
        ):
            result = remove_cherrypicked_samples(config, [])
            assert result == testing_samples


# ----- test update_filtered_positive_fields method -----


def test_update_filtered_positive_fields_assigns_expected_filtered_positive_fields():
    samples = [{}, {}]
    timestamp = datetime.now()
    version = "v2.3"
    mock_positive_identifier = MagicMock()
    mock_positive_identifier.is_positive.return_value = True
    mock_positive_identifier.version = version

    update_filtered_positive_fields(mock_positive_identifier, samples, version, timestamp)
    for sample in samples:
        assert sample[FIELD_FILTERED_POSITIVE] is True
        assert sample[FIELD_FILTERED_POSITIVE_VERSION] == version
        assert sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] == timestamp


# ----- test update_mongo_filtered_positive_fields method -----


def test_update_mongo_filtered_positive_fields_raises_with_error_updating_mongo(config, mock_mongo_collection):
    mock_mongo_collection().update_many.side_effect = ValueError("Boom!")
    with pytest.raises(ValueError):
        update_mongo_filtered_positive_fields(config, [], "v2.3", None)


def test_update_mongo_filtered_positive_fields_updates_expected_samples(
    config, testing_samples, samples_collection_accessor
):
    version = "v2.3"
    timestamp = datetime.now()
    updated_samples = testing_samples[:3]
    updated_samples[0][FIELD_FILTERED_POSITIVE] = True
    updated_samples[1][FIELD_FILTERED_POSITIVE] = False
    updated_samples[2][FIELD_FILTERED_POSITIVE] = False

    result = update_mongo_filtered_positive_fields(config, updated_samples, version, timestamp)
    assert result is True

    assert samples_collection_accessor.count_documents({}) == len(testing_samples)
    # ensure samples in mongo are updated as expected
    for sample in samples_collection_accessor.find({FIELD_MONGODB_ID: updated_samples[0][FIELD_MONGODB_ID]}):
        assert sample[FIELD_FILTERED_POSITIVE] is True
        assert sample[FIELD_FILTERED_POSITIVE_VERSION] == version
        assert sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] is not None

    for sample in samples_collection_accessor.find(
        {FIELD_MONGODB_ID: {"$in": [updated_samples[1][FIELD_MONGODB_ID], updated_samples[2][FIELD_MONGODB_ID]]}}
    ):
        assert sample[FIELD_FILTERED_POSITIVE] is False
        assert sample[FIELD_FILTERED_POSITIVE_VERSION] == version
        assert sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] is not None


# ----- test update_mlwh_filtered_positive_fields method -----


def test_update_mlwh_filtered_positive_fields_return_false_with_no_connection(config):
    with patch("migrations.helpers.update_filtered_positives_helper.create_mysql_connection") as mock_connection:
        mock_connection().is_connected.return_value = False
        result = update_mlwh_filtered_positive_fields(config, [])
        assert result is False


def test_update_mlwh_filtered_positive_fields_raises_with_error_updating_mlwh(config, mlwh_connection):
    with patch(
        "migrations.helpers.update_filtered_positives_helper.run_mysql_executemany_query",
        side_effect=NotImplementedError("Boom!"),
    ):
        with pytest.raises(NotImplementedError):
            update_mlwh_filtered_positive_fields(config, [])


def test_update_mlwh_filtered_positive_fields_calls_to_update_samples(config, mlwh_connection):
    # populate the mlwh database with existing entries
    mlwh_samples = [
        {
            MLWH_MONGODB_ID: "1",
            MLWH_COORDINATE: "A1",
            MLWH_PLATE_BARCODE: "123",
            MLWH_ROOT_SAMPLE_ID: "MCM001",
            MLWH_RNA_ID: "AAA123",
            MLWH_RESULT: POSITIVE_RESULT_VALUE,
            MLWH_FILTERED_POSITIVE: None,
            MLWH_FILTERED_POSITIVE_VERSION: None,
            MLWH_FILTERED_POSITIVE_TIMESTAMP: None,
        },
        {
            MLWH_MONGODB_ID: "2",
            MLWH_COORDINATE: "B1",
            MLWH_PLATE_BARCODE: "123",
            MLWH_ROOT_SAMPLE_ID: "MCM002",
            MLWH_RNA_ID: "BBB123",
            MLWH_RESULT: POSITIVE_RESULT_VALUE,
            MLWH_FILTERED_POSITIVE: True,
            MLWH_FILTERED_POSITIVE_VERSION: "v1.0",
            MLWH_FILTERED_POSITIVE_TIMESTAMP: datetime(2020, 4, 23, 14, 40, 8),
        },
    ]
    insert_sql = """\
    INSERT INTO lighthouse_sample (mongodb_id, root_sample_id, rna_id, plate_barcode, coordinate,
    result, filtered_positive, filtered_positive_version, filtered_positive_timestamp)
    VALUES (%(mongodb_id)s, %(root_sample_id)s, %(rna_id)s, %(plate_barcode)s, %(coordinate)s,
    %(result)s, %(filtered_positive)s, %(filtered_positive_version)s,
    %(filtered_positive_timestamp)s)
    """
    cursor = mlwh_connection.cursor()
    cursor.executemany(insert_sql, mlwh_samples)
    cursor.close()
    mlwh_connection.commit()

    # call to update the database with newly filtered positive entries
    update_timestamp = datetime(2020, 6, 23, 14, 40, 8)
    mongo_samples = [
        {
            FIELD_MONGODB_ID: "1",
            FIELD_COORDINATE: "A01",
            FIELD_PLATE_BARCODE: "123",
            FIELD_ROOT_SAMPLE_ID: "MCM001",
            FIELD_RNA_ID: "AAA123",
            FIELD_FILTERED_POSITIVE: True,
            FIELD_FILTERED_POSITIVE_VERSION: "v2.3",
            FIELD_FILTERED_POSITIVE_TIMESTAMP: update_timestamp,
        },
        {
            FIELD_MONGODB_ID: "2",
            FIELD_COORDINATE: "B01",
            FIELD_PLATE_BARCODE: "123",
            FIELD_ROOT_SAMPLE_ID: "MCM002",
            FIELD_RNA_ID: "BBB123",
            FIELD_FILTERED_POSITIVE: False,
            FIELD_FILTERED_POSITIVE_VERSION: "v2.3",
            FIELD_FILTERED_POSITIVE_TIMESTAMP: update_timestamp,
        },
    ]

    result = update_mlwh_filtered_positive_fields(config, mongo_samples)
    assert result is True

    cursor = mlwh_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM lighthouse_sample")
    sample_count = cursor.fetchone()[0]
    cursor.execute(
        "SELECT filtered_positive, filtered_positive_version, filtered_positive_timestamp FROM "
        "lighthouse_sample WHERE mongodb_id = '1'"
    )
    filtered_positive_sample = cursor.fetchone()
    cursor.execute(
        "SELECT filtered_positive, filtered_positive_version, filtered_positive_timestamp FROM "
        "lighthouse_sample WHERE mongodb_id = '2'"
    )
    filtered_negative_sample = cursor.fetchone()
    cursor.close()

    assert sample_count == 2
    assert filtered_positive_sample[0]
    assert filtered_positive_sample[1] == "v2.3"
    assert filtered_positive_sample[2] == update_timestamp
    assert not filtered_negative_sample[0]
    assert filtered_negative_sample[1] == "v2.3"
    assert filtered_negative_sample[2] == update_timestamp


# ----- test biomek_labclass_by_centre_name method -----


def test_biomek_labclass_by_centre_name(config):
    centres = [
        {"name": "test centre 1", "biomek_labware_class": "test class 1"},
        {"name": "test centre 2", "biomek_labware_class": "test class 2"},
    ]
    labclass_by_name = biomek_labclass_by_centre_name(centres)

    assert len(labclass_by_name.keys()) == 2
    assert labclass_by_name["test centre 1"] == "test class 1"
    assert labclass_by_name["test centre 2"] == "test class 2"


# ----- test update_dart_filtered_positive_fields method -----


def test_update_dart_filtered_positive_fields_throws_with_error_connecting_to_dart(config, mock_dart_conn):
    mock_dart_conn.side_effect = NotImplementedError("Boom!")
    with pytest.raises(Exception):
        update_dart_fields(config, [])


def test_update_dart_filtered_positive_fields_throws_no_dart_connection(config, mock_dart_conn):
    mock_dart_conn.return_value = None
    with pytest.raises(ValueError):
        update_dart_fields(config, [])


def test_update_dart_filtered_positive_fields_returns_false_with_error_creating_cursor(config, mock_dart_conn):
    mock_dart_conn().cursor.side_effect = NotImplementedError("Boom!")
    result = update_dart_fields(config, [])
    assert result is False


def test_update_dart_filtered_positive_fields_returns_false_error_adding_plate(config, mock_dart_conn):
    with patch(
        "migrations.helpers.update_filtered_positives_helper.add_dart_plate_if_doesnt_exist",
        side_effect=Exception("Boom!"),
    ):
        samples = [{FIELD_PLATE_BARCODE: "123", FIELD_SOURCE: config.CENTRES[0]["name"]}]
        result = update_dart_fields(config, samples)
        assert result is False


def test_update_dart_filtered_positive_fields_non_pending_plate_does_not_update_wells(config, mock_dart_conn):
    with patch(
        "migrations.helpers.update_filtered_positives_helper.add_dart_plate_if_doesnt_exist",
        return_value="not pending",
    ):
        with patch(
            "migrations.helpers.update_filtered_positives_helper.set_dart_well_properties"
        ) as mock_update_well_props:
            samples = [{FIELD_PLATE_BARCODE: "123", FIELD_SOURCE: config.CENTRES[0]["name"]}]
            result = update_dart_fields(config, samples)

            mock_dart_conn().cursor().commit.assert_called_once()
            mock_update_well_props.assert_not_called()
            assert result is True


def test_update_dart_filtered_positive_fields_returns_false_unable_to_determine_well_index(config, mock_dart_conn):
    with patch(
        "migrations.helpers.update_filtered_positives_helper.add_dart_plate_if_doesnt_exist",
        return_value=DART_STATE_PENDING,
    ):
        with patch(
            "migrations.helpers.update_filtered_positives_helper.get_dart_well_index",
            return_value=None,
        ):
            with patch(
                "migrations.helpers.update_filtered_positives_helper.set_dart_well_properties"
            ) as mock_update_well_props:
                samples = [{FIELD_PLATE_BARCODE: "123", FIELD_SOURCE: config.CENTRES[0]["name"]}]
                result = update_dart_fields(config, samples)

                mock_dart_conn().cursor().rollback.assert_called_once()
                mock_update_well_props.assert_not_called()
                assert result is False


def test_update_dart_filtered_positive_fields_returns_false_error_mapping_to_well_props(config, mock_dart_conn):
    with patch(
        "migrations.helpers.update_filtered_positives_helper.add_dart_plate_if_doesnt_exist",
        return_value=DART_STATE_PENDING,
    ):
        with patch(
            "migrations.helpers.update_filtered_positives_helper.get_dart_well_index",
            return_value=None,
        ):
            with patch(
                "migrations.helpers.update_filtered_positives_helper.map_mongo_doc_to_dart_well_props",  # noqa: E501
                side_effect=Exception("Boom!"),
            ):
                with patch(
                    "migrations.helpers.update_filtered_positives_helper.set_dart_well_properties"
                ) as mock_update_well_props:
                    samples = [{FIELD_PLATE_BARCODE: "123", FIELD_SOURCE: config.CENTRES[0]["name"]}]
                    result = update_dart_fields(config, samples)

                    mock_dart_conn().cursor().rollback.assert_called_once()
                    mock_update_well_props.assert_not_called()
                    assert result is False


def test_update_dart_filtered_positive_fields_returns_false_error_adding_well_properties(config, mock_dart_conn):
    with patch(
        "migrations.helpers.update_filtered_positives_helper.add_dart_plate_if_doesnt_exist",
        return_value=DART_STATE_PENDING,
    ):
        with patch(
            "migrations.helpers.update_filtered_positives_helper.get_dart_well_index",
            return_value=12,
        ):
            with patch(
                "migrations.helpers.update_filtered_positives_helper.map_mongo_doc_to_dart_well_props"  # noqa: E501
            ):
                with patch(
                    "migrations.helpers.update_filtered_positives_helper.set_dart_well_properties",
                    side_effect=NotImplementedError("Boom!"),
                ):
                    samples = [{FIELD_PLATE_BARCODE: "123", FIELD_SOURCE: config.CENTRES[0]["name"]}]
                    result = update_dart_fields(config, samples)

                    mock_dart_conn().cursor().rollback.assert_called_once()
                    assert result is False


def test_update_dart_filtered_positive_fields_returns_true_multiple_new_plates(config, mock_dart_conn):
    with patch("migrations.helpers.update_filtered_positives_helper.add_dart_plate_if_doesnt_exist") as mock_add_plate:
        mock_add_plate.return_value = DART_STATE_PENDING
        with patch("migrations.helpers.update_filtered_positives_helper.get_dart_well_index") as mock_get_well_index:
            test_well_index = 12
            mock_get_well_index.return_value = test_well_index
            with patch(
                "migrations.helpers.update_filtered_positives_helper.map_mongo_doc_to_dart_well_props"  # noqa: E501
            ) as mock_map:
                test_well_props = {"prop1": "value1", "test prop": "test value"}
                mock_map.return_value = test_well_props
                with patch(
                    "migrations.helpers.update_filtered_positives_helper.set_dart_well_properties"
                ) as mock_set_well_props:
                    test_centre_name = config.CENTRES[0]["name"]
                    test_labware_class = config.CENTRES[0]["biomek_labware_class"]
                    samples = [
                        {
                            FIELD_PLATE_BARCODE: "123",
                            FIELD_SOURCE: test_centre_name,
                            FIELD_COORDINATE: "A01",
                        },
                        {
                            FIELD_PLATE_BARCODE: "ABC",
                            FIELD_SOURCE: test_centre_name,
                            FIELD_COORDINATE: "B03",
                        },
                        {
                            FIELD_PLATE_BARCODE: "XYZ",
                            FIELD_SOURCE: test_centre_name,
                            FIELD_COORDINATE: "E11",
                        },
                    ]

                    result = update_dart_fields(config, samples)

                    num_samples = len(samples)
                    assert mock_add_plate.call_count == num_samples
                    assert mock_get_well_index.call_count == num_samples
                    assert mock_map.call_count == num_samples
                    assert mock_set_well_props.call_count == num_samples
                    for sample in samples:
                        mock_add_plate.assert_any_call(
                            mock_dart_conn().cursor(),
                            sample[FIELD_PLATE_BARCODE],
                            test_labware_class,
                        )
                        mock_get_well_index.assert_any_call(sample[FIELD_COORDINATE])
                        mock_map.assert_any_call(sample)
                        mock_set_well_props.assert_any_call(
                            mock_dart_conn().cursor(),
                            sample[FIELD_PLATE_BARCODE],
                            test_well_props,
                            test_well_index,
                        )
                    assert mock_dart_conn().cursor().commit.call_count == num_samples
                    assert result is True


def test_update_dart_filtered_positive_fields_returns_true_single_new_plate_multiple_wells(config, mock_dart_conn):
    with patch("migrations.helpers.update_filtered_positives_helper.add_dart_plate_if_doesnt_exist") as mock_add_plate:
        mock_add_plate.return_value = DART_STATE_PENDING
        with patch("migrations.helpers.update_filtered_positives_helper.get_dart_well_index") as mock_get_well_index:
            test_well_index = 12
            mock_get_well_index.return_value = test_well_index
            with patch(
                "migrations.helpers.update_filtered_positives_helper.map_mongo_doc_to_dart_well_props"  # noqa: E501
            ) as mock_map:
                test_well_props = {"prop1": "value1", "test prop": "test value"}
                mock_map.return_value = test_well_props
                with patch(
                    "migrations.helpers.update_filtered_positives_helper.set_dart_well_properties"
                ) as mock_set_well_props:
                    test_plate_barcode = "123"
                    test_centre_name = config.CENTRES[0]["name"]
                    test_labware_class = config.CENTRES[0]["biomek_labware_class"]
                    samples = [
                        {
                            FIELD_PLATE_BARCODE: test_plate_barcode,
                            FIELD_SOURCE: test_centre_name,
                            FIELD_COORDINATE: "A01",
                        },
                        {
                            FIELD_PLATE_BARCODE: test_plate_barcode,
                            FIELD_SOURCE: test_centre_name,
                            FIELD_COORDINATE: "B03",
                        },
                        {
                            FIELD_PLATE_BARCODE: test_plate_barcode,
                            FIELD_SOURCE: test_centre_name,
                            FIELD_COORDINATE: "E11",
                        },
                    ]

                    result = update_dart_fields(config, samples)

                    mock_add_plate.assert_called_once_with(
                        mock_dart_conn().cursor(), test_plate_barcode, test_labware_class
                    )

                    num_samples = len(samples)
                    assert mock_get_well_index.call_count == num_samples
                    assert mock_map.call_count == num_samples
                    assert mock_set_well_props.call_count == num_samples
                    for sample in samples:
                        mock_get_well_index.assert_any_call(sample[FIELD_COORDINATE])
                        mock_map.assert_any_call(sample)
                        mock_set_well_props.assert_any_call(
                            mock_dart_conn().cursor(),
                            sample[FIELD_PLATE_BARCODE],
                            test_well_props,
                            test_well_index,
                        )
                    assert mock_dart_conn().cursor().commit.call_count == 1
                    assert result is True
