import datetime
import re
import logging

logger = logging.getLogger(__name__)

CREATED_DATE_FIELD_NAME = 'First Imported Date'

def add_timestamps_to_samples(db):
  logger.debug('Starting...')

  collection_name_to_timestamp = map_collection_name_to_timestamp(db)

  # order the collections chronologically (oldest first)
  # so that when we loop through, the timestamp we set is when the sample first appeared in a collection
  collection_names_chrono_order = sorted(collection_name_to_timestamp)
  logger.debug(f'Collections in chronological order: {collection_names_chrono_order}')

  processed_samples = set()

  # find all root sample ids in this collection that we have not already processed
  # TODO: switch to using concatentation of root sample id & 3 other fields for uniqueness?
  for collection_name in collection_names_chrono_order:
    logger.debug('Processing archived collection: ' + collection_name)

    root_sample_ids = []
    for sample in db[collection_name].find():
      root_sample_id = sample['Root Sample ID']
      if root_sample_id in processed_samples: continue

      root_sample_ids.append(root_sample_id)
      processed_samples.add(root_sample_id) # pylint doesn't like it because it thinks it's a dict, not a set

    logger.debug(f'Num root sample ids to process: {len(root_sample_ids)}')

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
    logger.debug(f'Result - modified count: {result.modified_count}')


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


# for testing queries only, not for prod code
# def query_samples(db, root_sample_ids):
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

  # import pdb; pdb.set_trace()
  # logger.debug(f'Result - modified count: {result.modified_count}')