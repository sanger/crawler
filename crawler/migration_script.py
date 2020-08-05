import datetime
import re
import logging

import pymongo
from pymongo.errors import BulkWriteError

from crawler.db import (
    create_mongo_client,
    get_mongo_db
)
from crawler.helpers import (
    get_config
)

logger = logging.getLogger(__name__)

CREATED_DATE_FIELD_NAME = 'First Imported Date'

def run(settings_module: str = "") -> None:
  print('run')
  config, settings_module = get_config(settings_module)
  print('got config')


  with create_mongo_client(config) as client:
    print('created mongo client')
    db = get_mongo_db(config, client)
    print('got mongo db')

    query_samples(db, 'samples_200728_1003')
    # query_samples(db, 'samples')
    print('queried samples')





def add_timestamps_to_samples(db):
  print('Starting...')

  collection_name_to_timestamp = map_collection_name_to_timestamp(db)

  # order the collections chronologically (oldest first)
  # so that when we loop through, the timestamp we set is when the sample first appeared in a collection
  collection_names_chrono_order = sorted(collection_name_to_timestamp)
  print(f'Collections in chronological order: {collection_names_chrono_order}')

  processed_samples = set()

  # find all root sample ids in this collection that we have not already processed
  # TODO: switch to using concatentation of root sample id & 3 other fields for uniqueness?
  # TODO: save these to a file in case of partial success?
  for collection_name in collection_names_chrono_order:
    print('Processing archived collection: ' + collection_name)

    root_sample_ids = []
    for sample in db[collection_name].find():
      root_sample_id = sample['Root Sample ID']
      if root_sample_id in processed_samples: continue

      root_sample_ids.append(root_sample_id)
      processed_samples.add(root_sample_id)

    print(f'Num root sample ids to process: {len(root_sample_ids)}')

    # update documents in the main samples collection
    # where the root sample id matches those in this collection
    # technically Root Sample ID is not unique so this could bring back multiple per sample
    # unique combo is Root Sample ID, RNA ID, Result and Lab ID
    # But to match on them, would have to query one by one
    # and I think it's OK to set the date as the same for 'duplicates' of the sample...?
    # TODO: is it OK? Might be useful to have different timestamps for the duplicates to work out what happened.
    # and the 'First Imported Date' has not yet been set, to avoid overwriting it
    # '$set' syntax is important, otherwise it overwrites the whole document!
    # TODO: check why _created and _updated are 1970, and whether I can insert into _created here, to back-fill data
    result = db.samples.update_many(
      { 'Root Sample ID': { '$in': root_sample_ids } },
      { '$set': { CREATED_DATE_FIELD_NAME: format_date(collection_name_to_timestamp[collection_name]) } }
    )

    # import pdb; pdb.set_trace()
    print(f'Result - modified count: {result.modified_count}')


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
  return bool(re.search(r'samples_(\d){6}_(\d){4}', collection_name))


def format_date(date):
  return date.strftime('%Y-%m-%d %H:%M:%S %Z') # example: 2020-05-20 15:10:00 UTC



def query_samples(db, collection_name):
  print(f'Time start: {datetime.datetime.now()}')

  # update collection with new aggregate column
  # TODO: this will not work if one of these fields is null
  update_result_1 = db[collection_name].update_many(
    { },
    [
      { '$set': { 'concat_id': { '$concat': [ "$Root Sample ID", " - ", "$RNA ID", " - ", "$Result", " - ", "$Lab ID" ] } } }
    ]
  )
  print(f'Time after adding field to collection: {datetime.datetime.now()}')
  print('update_result_1 modified: ', update_result_1.modified_count)

  # retrieve column and put into a list
  concat_ids = []
  concat_ids_result = db[collection_name].find({ }, { 'concat_id': True })
  for sample in concat_ids_result:
    concat_ids.append(sample['concat_id'])
  print('Number of concat_ids in collection: ', len(concat_ids))
  print(f'Time after querying field and building list: {datetime.datetime.now()}')

  # update samples collection with new aggregate column
  update_result_2 = db.samples.update_many(
    { },
    [
      { '$set': { 'concat_id': { '$concat': [ "$Root Sample ID", " - ", "$RNA ID", " - ", "$Result", " - ", "$Lab ID" ] } } }
    ]
  )
  print(f'Time after adding field to samples collection: {datetime.datetime.now()}')
  print('update_result_2 modified: ', update_result_2.modified_count)

  # do update, where column is in list
  update_result_3 = db.samples.update_many(
    { 'concat_id': { '$in': concat_ids } },
    { '$set': { 'test field': 'test date' } }
  )
  print(f'Time after querying based on new field and updating with timestamp: {datetime.datetime.now()}')
  print('update_result_3 modified: ', update_result_3.modified_count)




# def query_samples(db, collection_name):
#   print(f'Time start: {datetime.datetime.now()}')

#   matching_criteria = []
#   concat_ids_list = db[collection_name].aggregate([
#     { '$project': { 'itemDescription': { '$concat': [ "$Root Sample ID", " - ", "$RNA ID", " - ", "$Result", " - ", "$Lab ID" ] } } }
#   ])
#   print('concat_ids_list.next(): ', concat_ids_list.next())
#   for record in concat_ids_list:
#     # print(record)
#     matching_criteria.append(record['itemDescription'])
#   print(f'Time after building matching criteria: {datetime.datetime.now()}')
#   print(f'Length matching criteria : {len(matching_criteria)}')

#   # matching_criteria = ['HUL00011540 - AP-rna-00020176_A01 - Void - AP']
#   # matching_criteria = ['TLS00000001 - TL-rna-00000001_A01 - Positive - 2020-05-10 14:01:00 UTC - TLS - Test Lab Somewhere']
#   matching_sample_ids = []
#   matching_samples = db.samples.find({}).aggregate([
#     { '$project': { 'itemDescription': { '$concat': [ "$Root Sample ID", " - ", "$RNA ID", " - ", "$Result", " - ", "$Lab ID" ] } } },
#     { '$match': { 'itemDescription': { '$in': matching_criteria } } }
#   ])
#   print(f'Time after all queries: {datetime.datetime.now()}')
#   for sample in matching_samples:
#     matching_sample_ids.append(sample['_id'])
#   print(f'Time after building matching sample ids: {datetime.datetime.now()}')
#   print('matching sample ids: ', len(matching_sample_ids))

#   # matching_samples =
#   # result = db.samples.update_many(
#   #   { },
#   #   [
#   #     { '$project': { 'itemDescription': { '$concat': [ "$Root Sample ID", " - ", "$RNA ID", " - ", "$Result", " - ", "$Lab ID" ] } } },
#   #     { '$match': { 'itemDescription': { '$in': matching_criteria } } },
#   #     { '$set': { 'New timestamp field': 'testing testing' } }
#   #   ]
#   # )
#   # print(f'Time after all updates: {datetime.datetime.now()}')
#   # # print('result.next(): ', result.next())
#   # for record in result: print('hi', record)







# for testing queries only, not for prod code
# def query_samples(db, collection_name):
#   print(f'Time start: {datetime.datetime.now()}')

#   matching_criteria = []
#   for sample in db[collection_name].find():
#     matching_criteria.append({
#       'Root Sample ID': sample['Root Sample ID'],
#       'RNA ID': sample['RNA ID'],
#       'Result': sample['Result'],
#       'Lab ID': sample['Lab ID'],
#     })
#   print(f'Time after building matching criteria: {datetime.datetime.now()}')
#   print(f'Length matching criteria : {len(matching_criteria)}')
#   # print(f'matching_criteria: {matching_criteria}')

#   for matching_criterion in matching_criteria:
#     result = db.samples.find(matching_criterion)
#     print(f'Result: {result.count()}')
#     # for record in result: print(f'Result: {record}')
#   print(f'Time after all queries: {datetime.datetime.now()}')

#   # import pdb; pdb.set_trace()
#   # print(f'Result - modified count: {result.modified_count}')




# result = db.samples.find(
  #   {
  #     '$and': [
  #       { 'Root Sample ID': { '$in': root_sample_ids } },
  #       { '$or': [
  #         { 'First Imported Date': None },
  #         { 'First Imported Date': { '$lt': CODE_UPDATED_DATE } }
  #       ]}
  #     ]
  #   }
  # )