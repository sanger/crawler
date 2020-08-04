import datetime
import re

def add_timestamps_to_samples(mongo_database):
  print('DEBUG: add_timestamps_to_samples')
  db = mongo_database

  collection_name_to_timestamp = {}

  for collection_name in db.list_collection_names():
    if not is_sample_archive_collection(collection_name):
      continue

    timestamp = extract_timestamp(collection_name)
    collection_name_to_timestamp[collection_name] = timestamp

  print('DEBUG: collection_name_to_timestamp: ', collection_name_to_timestamp)

  # loop through the collections in chronological order
  # so that the timestamp we set is when the sample first appeared in a collection
  collection_names_chrono_order = sorted(collection_name_to_timestamp)
  print('DEBUG: collection_names_chrono_order: ', collection_names_chrono_order)

  for collection_name in collection_names_chrono_order:
    print('DEBUG: Processing collection: ' + collection_name)

    timestamp = collection_name_to_timestamp[collection_name]
    date_time_formatted = timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')

    coll = db[collection_name]
    root_sample_ids = []
    for sample in coll.find():
      root_sample_ids.append(sample['Root Sample ID'])

    print('DEBUG: root_sample_ids: ', root_sample_ids)

    # update documents in the main samples collection
    # where the root sample id matches those in this collection
    # and the 'First Imported Date' has not yet been set, to avoid overwriting it
    result = db.samples.update_many( { 'Root Sample ID': { '$in': root_sample_ids }, 'First Imported Date': None }, { '$set': { 'First Imported Date': date_time_formatted } } )

    # import pdb; pdb.set_trace()
    print('DEBUG: result.modified_count: ', result.modified_count)


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

def is_sample_archive_collection(collection_name):
  if re.search(r'samples_(\d){6}_(\d){4}', collection_name):
    print('DEBUG: Matches')
    return True
  else:
    print('DEBUG: No match')
    return False