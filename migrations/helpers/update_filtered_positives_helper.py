from datetime import datetime
from types import ModuleType
from typing import List, Dict, Any
from migrations.helpers.shared_helper import print_exception
from crawler.db import (
    create_dart_sql_server_conn,
    create_mongo_client,
    get_mongo_db,
    get_mongo_collection,
    create_mysql_connection,
    run_mysql_executemany_query,
    add_dart_plate_if_doesnt_exist,
    set_dart_well_properties,
)
from crawler.constants import (
    COLLECTION_SAMPLES,
    COLLECTION_CENTRES,
    FIELD_MONGODB_ID,
    FIELD_RESULT,
    FIELD_PLATE_BARCODE,
    FIELD_SOURCE,
    FIELD_COORDINATE,
    FIELD_ROOT_SAMPLE_ID,
    POSITIVE_RESULT_VALUE,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    DART_STATE_PENDING,
)
from crawler.filtered_positive_identifier import FilteredPositiveIdentifier
from crawler.sql_queries import (
    SQL_DART_GET_PLATE_BARCODES,
    SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE,
)
from crawler.helpers import (
    map_mongo_to_sql_common,
    get_dart_well_index,
    map_mongo_doc_to_dart_well_props,
)

def update_filtered_positives(config):
    """Updates filtered positive values for all positive samples in pending plates

        Arguments:
            config {ModuleType} -- application config specifying database details
    """
    num_pending_plates = 0
    num_positive_pending_samples = 0
    mongo_updated = False
    mlwh_updated = False
    dart_updated = False
    try:
        # Get barcodes of pending plates in DART
        print("Selecting pending plates from DART...")
        pending_plate_barcodes = pending_plate_barcodes_from_dart(config)
        num_pending_plates = len(pending_plate_barcodes)
        print(f"{len(pending_plate_barcodes)} pending plates found in DART")

        if num_pending_plates > 0:
            # Get positive result samples from Mongo in these pending plates
            print("Selecting postive samples in pending plates from Mongo...")
            positive_pending_samples = positive_result_samples_from_mongo(config, pending_plate_barcodes)
            num_positive_pending_samples = len(positive_pending_samples)
            print(f"{num_positive_pending_samples} positive samples in pending plates found in Mongo")

            if num_positive_pending_samples > 0:
                filtered_positive_identifier = FilteredPositiveIdentifier()
                version = filtered_positive_identifier.current_version()
                update_timestamp = datetime.now()
                print("Updating filtered positives...")
                update_filtered_positive_fields(filtered_positive_identifier, positive_pending_samples, version, update_timestamp)
                print("Updated filtered positives")

                print("Updating Mongo...")
                mongo_updated = update_samples_in_mongo(config, positive_pending_samples, version, update_timestamp)
                print("Finished updating Mongo")

                print("Updating MLWH...")
                mlwh_updated = update_samples_in_mlwh(config, positive_pending_samples)
                print("Finished updating MLWH")

                print("Updating DART...")
                dart_updated = update_samples_in_dart(config, positive_pending_samples)
                print("Finished updating DART")
            else:
                print("No positive samples in pending plates found in Mongo, not updating any database")
        else:
            print("No pending plates found in DART, not updating any database")
        
    except Exception as e:
        print("---------- Process aborted: ----------")
        print_exception()
    finally:
        print_processing_status(num_pending_plates, num_positive_pending_samples, mongo_updated, mlwh_updated, dart_updated)

def pending_plate_barcodes_from_dart(config: ModuleType):
    """Fetch the barcodes of all plates from DART that in the 'pending' state

        Arguments:
            config {ModuleType} -- application config specifying database details

        Returns:
            List[str] -- barcodes of pending plates
    """
    sql_server_connection = create_dart_sql_server_conn(config, True)
    if  sql_server_connection is None:
        # to be caught by calling method
        raise ValueError('Unable to establish DART SQL Server connection')
    
    plate_barcodes = []
    cursor = sql_server_connection.cursor()
    
    try:
        rows = cursor.execute(SQL_DART_GET_PLATE_BARCODES, DART_STATE_PENDING).fetchall()
        plate_barcodes = [row[0] for row in rows]
    except Exception as e:
        print_exception()
    finally:
        sql_server_connection.close()

    return plate_barcodes

def positive_result_samples_from_mongo(config: ModuleType, plate_barcodes: List[str]) -> List[Dict[str, Any]]:
    """Fetch positive samples from Mongo contained within specified plates.

        Arguments:
            config {ModuleType} -- application config specifying database details
            plate_barcodes {List[str]} -- barcodes of plates whose samples we are concerned with

        Returns:
            List[Dict[str, str]] -- List of positive samples contained within specified plates
    """
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)
        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)

        # this should take everything from the cursor find into RAM memory (assuming you have enough memory)
        # should we project to an object that has fewer fields?
        return list(
            samples_collection.find({
                FIELD_RESULT: { '$eq': POSITIVE_RESULT_VALUE },
                FIELD_PLATE_BARCODE : { '$in': plate_barcodes }
            })
        )

def update_filtered_positive_fields(filtered_positive_identifier: FilteredPositiveIdentifier, samples: List[Dict[str, Any]], version: str, update_timestamp: datetime) -> None:
    """Updates filtered positive fields on all passed-in samples

        Arguments:
            filtered_positive_identifier {FilteredPositiveIdentifier} -- the identifier through which to pass samples to determine whether they are filtered positive
            samples {List[Dict[str, str]]} -- the list of samples for which to re-determine filtered positive values
            version {str} -- the filtered positive identifier version used
            update_timestamp {datetime} -- the timestamp at which the update was performed
    """
    version = filtered_positive_identifier.current_version()
    # Expect all samples to be passed into here to have a positive result
    for sample in samples:
        sample[FIELD_FILTERED_POSITIVE] = filtered_positive_identifier.is_positive(sample)
        sample[FIELD_FILTERED_POSITIVE_VERSION] = version
        sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] = update_timestamp

def update_samples_in_mongo(config: ModuleType, samples: List[Dict[str, Any]], version: str, update_timestamp: datetime) -> bool:
    """Bulk updates sample filtered positive fields in the Mongo database

        Arguments:
            config {ModuleType} -- application config specifying database details
            samples {List[Dict[str, str]]} -- the list of samples whose filtered positive fields should be updated
            version {str} -- the filtered positive identifier version used
            update_timestamp {datetime} -- the timestamp at which the update was performed

        Returns:
            bool -- whether the updates completed successfully
    """
    with create_mongo_client(config) as client:
        mongo_db = get_mongo_db(config, client)

        # get ids of those that are filtered positive, and those that aren't
        all_ids = [sample[FIELD_MONGODB_ID] for sample in samples]
        filtered_positive_ids = [sample[FIELD_MONGODB_ID] for sample in list(filter(lambda x: x[FIELD_FILTERED_POSITIVE] == True, samples))]
        filtered_negative_ids = [mongo_id for mongo_id in all_ids if mongo_id not in filtered_positive_ids]

        samples_collection = get_mongo_collection(mongo_db, COLLECTION_SAMPLES)
        samples_collection.update_many(
            { FIELD_MONGODB_ID: { '$in': filtered_positive_ids } },
            { "$set": { FIELD_FILTERED_POSITIVE: True, FIELD_FILTERED_POSITIVE_VERSION: version, FIELD_FILTERED_POSITIVE_TIMESTAMP: update_timestamp } })
        samples_collection.update_many(
            { FIELD_MONGODB_ID: { '$in': filtered_negative_ids } },
            { "$set": { FIELD_FILTERED_POSITIVE: False, FIELD_FILTERED_POSITIVE_VERSION: version, FIELD_FILTERED_POSITIVE_TIMESTAMP: update_timestamp } })
    return True

def update_samples_in_mlwh(config: ModuleType, samples: List[Dict[str, Any]]) -> bool:
    """Bulk updates sample filtered positive fields in the MLWH database

        Arguments:
            config {ModuleType} -- application config specifying database details
            samples {List[Dict[str, str]]} -- the list of samples whose filtered positive fields should be updated

        Returns:
            bool -- whether the updates completed successfully
    """
    mysql_conn = create_mysql_connection(config, False)

    if mysql_conn is not None and mysql_conn.is_connected():
        mlwh_samples = [map_mongo_to_sql_common(sample) for sample in samples]
        run_mysql_executemany_query(mysql_conn, SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE, mlwh_samples)
        return True
    else:
        return False

def update_samples_in_dart(config: ModuleType, samples: List[Dict[str, Any]]) -> bool:
    """Updates DART entries following updates to the filtered positive fields

        Arguments:
            config {ModuleType} -- application config specifying database details
            samples {List[Dict[str, str]]} -- the list of samples to update in DART

        Returns:
            bool -- whether the updates completed successfully
    """
    sql_server_connection = create_dart_sql_server_conn(config, True)
    if  sql_server_connection is None:
        raise ValueError('Unable to establish DART SQL Server connection')
    
    dart_updated_successfully = True
    centres = config.CENTRES  # type: ignore
    labclass_by_centre_name = biomek_labclass_by_centre_name(centres)
    try:
        cursor = sql_server_connection.cursor()

        for plate_barcode, samples_in_plate in groupby_transform(samples, lambda x: x[FIELD_PLATE_BARCODE]):  # type:ignore
            try:
                labware_class = labclass_by_centre_name[samples_in_plate[0][FIELD_SOURCE]]
                plate_state = add_dart_plate_if_doesnt_exist(cursor, plate_barcode, labware_class)
                if plate_state == DART_STATE_PENDING:
                    for sample in samples_in_plate:
                        well_index = get_dart_well_index(sample.get[FIELD_COORDINATE, None])
                        if well_index is not None:
                            well_props = map_mongo_doc_to_dart_well_props(sample)
                            set_dart_well_properties(cursor, plate_barcode, well_props, well_index)
                        else:
                            raise ValueError(f'Unable to determine DART well index for sample {sample[FIELD_ROOT_SAMPLE_ID]} in plate {plate_barcode}')
                cursor.commit()
                dart_updated_successfully &= True
            except:
                print(f"** Failed updating DART for samples in plate {plate_barcode} **")
                print_exception()
                cursor.rollback()
                dart_updated_successfully = False
    except:
        print_exception()
        dart_updated_successfully = False
    finally:
        sql_server_connection.close()

    return dart_updated_successfully

def biomek_labclass_by_centre_name(centres: List[Dict[str, str]]) -> Dict[str, str]:
    """Determines a mapping between centre name and biomek labware class.

        Arguments:
            centres {List[Dict[str, str]]} -- the list of all centres

        Returns:
            Dict[str, str] -- biomek labware class by centre name
    """
    class_by_name = {}
    for centre in centres:
        class_by_name[centre["name"]] = centre["biomek_labware_class"]
    return class_by_name

def print_processing_status(num_pending_plates: int, num_positive_pending_samples: int, mongo_updated: bool, mlwh_updated: bool, dart_updated: bool) -> None:
    """Prints the processing status of the update operation for each database, specifically whether entries were successfully updated

        Arguments:
            num_pending_plates {int} -- the number of pending plates found in DART
            num_positive_pending_samples {int} -- the number of samples in pending plates foudn in Mongo
            mongo_updated {bool} -- whether entries in the Mongo database were successfully updated
            mlwh_updated {bool} -- whether entries in the MLWH database were successfully updated
            datr_updated {bool} -- whether entries in the DART database were successfully updated
    """
    print("---------- Processing status of filtered positive rule changes: ----------")
    print(f"-- Found {num_pending_plates} pending plates in DART")
    print(f"-- Found {num_positive_pending_samples} samples in pending plates in Mongo")
    print(f"-- Mongo updated: {mongo_updated}")
    print(f"-- MLWH updated: {mlwh_updated}")
    print(f"-- DART updated: {dart_updated}")