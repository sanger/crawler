from datetime import datetime
from types import ModuleType
from typing import List, Dict
from migrations.helpers.shared_helper import print_exception
from crawler.db import (
    create_dart_sql_server_conn,
)
from crawler.constants import (
    COLLECTION_SAMPLES,
    FIELD_RESULT,
    FIELD_PLATE_BARCODE,
    POSITIVE_RESULT_VALUE,
)
from crawler.filtered_positive_identifier import FilteredPositiveIdentifier

def update_filtered_positives(config):
    """Updates filtered positive values for all positive samples in pending plates

        Arguments:
            config {ModuleType} -- application config specifying database details
    """
    # Get pending plate barcodes from DART - no way to do this yet
    # Pull all RESULT=positive samples from mongo with these plate barcodes - can probably do this. Get everything structured from mongo as we pass it to mongo
    # Re-determine whether filtered-positive - just pass what we get from mongo through the identifier
    # Update filtered-positive version etc. in mongo and MLWH
    # Re-upload well properties to DART - call insert_plates_and_wells_from_docs_into_dart?

    mongo_updated = False
    mlwh_updated = False
    dart_updated = False
    try:
      # Get barcodes of pending plates in DART
      print("Selecting pending plates from DART")
      pending_plate_barcodes = pending_plate_barcodes_from_dart(cursor)
      num_pending_plates = len(pending_plate_barcodes)
      print(f"{len(pending_plate_barcodes)} pending plates found in DART")

      if num_pending_plates > 0:
          # Get positive result samples from Mongo in these pending plates
          print("Selecting postive samples in pending plates from Mongo")
          positive_pending_samples = positive_result_samples_from_mongo(config, pending_plate_barcodes)
          num_positive_pending_samples = len(positive_pending_samples)
          print(f"{num_positive_pending_samples} positive samples in pending plates found in Mongo")

          if num_positive_pending_samples > 0:
              print("Updating filtered positives")
              update_filtered_positives(FilteredPositiveIdentifier(), positive_pending_samples, datetime.now())

              # update entries in mongo - throw if anything goes wrong, update flag if not
              # update entries in mlwh - throw if anything goes wrong, update flag if not
              # re-add to DART - throw if anything goes wrong, update flag if not
          else:
              print("No positive samples in pending plates found in Mongo, not updating any database")
      else:
          print("No pending plates found in DART, not updating any database")
      
    except Exception as e:
        print_exception()
    finally:
        print("---------- Processing status of filtered positive rule changes: ----------")
        print(f"-- Mongo updated: {mongo_updated}")
        print(f"-- MLWH updated: {mlwh_updated}")
        print(f"-- DART updated: {dart_updated}")

def pending_plate_barcodes_from_dart(config: ModuleType):
    """Fetch the barcodes of all plates from DART that in the 'pending' state

        Arguments:
            config {ModuleType} -- application config specifying database details

        Returns:
            List[str] -- barcodes of pending plates
    """
    sql_server_connection = create_dart_sql_server_conn(config, False)
    if  sql_server_connection is None:
        # to be caught by calling method
        raise ValueError('Unable to establish DART SQL Server connection')
    
    plate_barcodes = []
    cursor = sql_server_connection.cursor()
    
    try:
        # TODO - implement correctly once stored procedure is in place
        cursor.execute("{CALL dbo.plDART_PendingPlates}")
        plate_barcodes = cursor.commit()
    except Exception as e:
        # catch SQL Server cursor specific exceptions so we can rollback
        cursor.rollback()
        plate_barcodes = []
        print_exception()
    finally:
        sql_server_connection.close()

    return plate_barcodes

def positive_result_samples_from_mongo(config: ModuleType, plate_barcodes: List[str]) -> List[Dict[str, str]]:
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
        return list(
            samples_collection.find({
                '$and': [
                    { FIELD_RESULT: { '$eq': POSITIVE_RESULT_VALUE } },
                    { FIELD_PLATE_BARCODE : { '$in': plate_barcodes } }
                ]
            })
        )

def update_filtered_positives(filtered_positive_identifier: FilteredPositiveIdentifier, samples: List[Dict[str, str]], timestamp: datetime) -> None:
    """Updates filtered positive fields on all passed-in samples

        Arguments:
            filtered_positive_identifier {FilteredPositiveIdentifier} -- the identifier through which to pass samples to determine whether they are filtered positive
            samples {List[Dict[str, str]]} -- the list of samples for which to re-determine filtered positive values
            timestamp {Datetime} -- the current date/time
    """
    # Expect all samples to be passed into here to have a positive result
    for sample in samples:
        sample[FIELD_FILTERED_POSITIVE] = filtered_positive_identifier.is_positive(sample)
        sample[FIELD_FILTERED_POSITIVE_VERSION] = self.filtered_positive_identifier.current_version()
        sample[FIELD_FILTERED_POSITIVE_TIMESTAMP] = timestamp