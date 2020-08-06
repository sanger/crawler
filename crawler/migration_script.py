import datetime
import re
import sys
import traceback

import pymongo
from pymongo.errors import BulkWriteError

from crawler.db import (
    create_mongo_client,
    get_mongo_db
)
from crawler.helpers import (
    get_config
)

CREATED_DATE_FIELD_NAME = 'First Imported Date'
BATCH_SIZE = 250000


def run(settings_module: str = "") -> None:
  config, settings_module = get_config(settings_module)

  with create_mongo_client(config) as client:
    db = get_mongo_db(config, client)
    add_timestamps_to_samples(db)


# For updates, '$set' syntax is important, otherwise it overwrites the whole document!
# TODO: check why _created and _updated are 1970, and whether I can insert into _created here, to back-fill data
# TODO: put this file in a migrations folder
def add_timestamps_to_samples(db):
  print(f'Time start: {datetime.datetime.now()}')

  try:
    print('-- Update samples collection with new concatenated id column --')
    update_result_1 = db.samples.update_many(
      { },
      [
        { '$set': { 'concat_id': { '$concat': [ "$Root Sample ID", " - ", "$RNA ID", " - ", "$Result", " - ", "$Lab ID" ] } } }
      ]
    )
    print(f'Time after adding field to samples collection: {datetime.datetime.now()}')
    print('Number samples modified: ', update_result_1.modified_count)


    print('-- Order the collections chronologically (oldest first) --')
    # so that when we loop through, the timestamp we set is when the sample first appeared in a collection
    collection_name_to_timestamp = map_collection_name_to_timestamp(db)
    collection_names_chrono_order = sorted(collection_name_to_timestamp)
    print(f'Time after ordering collections: {datetime.datetime.now()}')
    print(f'Collections in chronological order: {collection_names_chrono_order}')


    processed_concat_ids = []

    for collection_name in collection_names_chrono_order:
      print(f'-- Starting processing collection: {collection_name} at {datetime.datetime.now()} --')

      print(f'-- Update collection {collection_name} with new concatenated id column --')
      # TODO: concat_id will be set to null if any of the constituent fields is null - do something about this?
      update_result_2 = db[collection_name].update_many(
        { },
        [
          { '$set': { 'concat_id': { '$concat': [ "$Root Sample ID", " - ", "$RNA ID", " - ", "$Result", " - ", "$Lab ID" ] } } }
        ]
      )
      print(f'Time after adding field to collection: {datetime.datetime.now()}')
      print('Number samples modified: ', update_result_2.modified_count)

      # TODO: save this to a file in case of partial success?
      print('-- Retrieve all concatenated ids for records we haven\'t processed yet --')
      concat_ids = []
      # TODO: add a condition that concat_id is not null?
      concat_ids_result = db[collection_name].find(
        { 'concat_id': { '$nin': processed_concat_ids } },
        { 'concat_id': True }
      )
      for sample in concat_ids_result:
        concat_ids.append(sample['concat_id'])
        processed_concat_ids.append(sample['concat_id'])

      print(f'Time after querying field and building list: {datetime.datetime.now()}')
      print(f'Total number of samples in collection: {db[collection_name].count()}')
      print(f'Of which, number to process: {len(concat_ids)}')


      print(f'-- Update samples collection with timestamps, where concat id matches, in batches of {BATCH_SIZE} --')
      # in batches, because otherwise get "Error: 'update' command document too large"

      num_batches = len(concat_ids) // BATCH_SIZE
      if len(concat_ids) % BATCH_SIZE != 0: num_batches += 1
      print(f'Number of batches: {num_batches}')

      for batch in range(num_batches):
        print(f'-- Starting batch {batch + 1} of {num_batches}: {datetime.datetime.now()} --')

        start = batch * BATCH_SIZE
        end = start + BATCH_SIZE # this will go out of bounds on the final batch but python is ok with that and just returns the remaining items
        print(f'Processing samples {start} to {end}')
        concat_ids_subset = concat_ids[start:end] # e.g. 0:250000, 250000:500000 etc.

        update_result_3 = db.samples.update_many(
          { 'concat_id': { '$in': concat_ids_subset } },
          { '$set': { CREATED_DATE_FIELD_NAME: format_date(collection_name_to_timestamp[collection_name]) } }
        )
        print(f'Time after querying based on new field and updating with timestamp: {datetime.datetime.now()}')
        print('Number samples modified: ', update_result_3.modified_count)
  except:
    print(f'An exception occurred, at {datetime.datetime.now()}')
    e = sys.exc_info()
    print(e[0]) # exception type
    print(e[1]) # exception message
    if e[2]: # traceback
      traceback.print_tb(e[2], limit=10)



def extract_timestamp(collection_name):
  _, date_string, time_string = collection_name.split('_')
  assert len(date_string) == 6
  assert len(time_string) == 4

  year = int('20' + date_string[0:2])
  month = int(date_string[2:4])
  day = int(date_string[4:6])
  hour = int(time_string[0:2])
  minute = int(time_string[2:4])
  second = 0
  microsecond = 0
  tzone = datetime.timezone.utc

  return datetime.datetime(year, month, day, hour, minute, second, microsecond, tzone)


def map_collection_name_to_timestamp(db):
  collection_name_to_timestamp = {}
  for collection_name in db.list_collection_names():
    if is_sample_archive_collection(collection_name):
      timestamp = extract_timestamp(collection_name)
      collection_name_to_timestamp[collection_name] = timestamp

  return collection_name_to_timestamp


def is_sample_archive_collection(collection_name):
  return bool(re.search(r'^samples_(\d){6}_(\d){4}$', collection_name))


def format_date(date):
  return date.strftime('%Y-%m-%d %H:%M:%S %Z') # example: 2020-05-20 15:10:00 UTC
