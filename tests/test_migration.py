from crawler.migration_script import ( add_timestamps_to_samples, query_samples, CREATED_DATE_FIELD_NAME )
from crawler.db import get_mongo_collection

def generate_example_samples(range):
  samples = []
  for n in range:
    samples.append({
      "Root Sample ID" : "TLS0000000" + str(n),
      "RNA ID" : "TL-rna-00000001_A01",
      "Result" : "Positive",
      "Date Tested" : "2020-05-10 14:01:00 UTC",
      "Lab ID" : "TLS",
      "source" : "Test Lab Somewhere"
    })

  return samples


def test_basic(mongo_database):
    _, db = mongo_database

    db.samples.insert_many(generate_example_samples(range(0, 4)))
    db.samples_200519_1510.insert_many(generate_example_samples(range(0, 2)))
    db.samples_200520_1510.insert_many(generate_example_samples(range(0, 4)))

    add_timestamps_to_samples(db)

    # TODO: switch to using count_documents as count() is deprecated
    total_samples = db.samples.count()
    samples_with_timestamp = db.samples.find( { CREATED_DATE_FIELD_NAME: { '$ne': None } } ).count()
    for sample in db.samples.find():
        print(f'DEBUG: sample: {sample}')
    assert total_samples == samples_with_timestamp

    from_samples_200519_1510 = db.samples.find( { 'Root Sample ID': { '$in': ['TLS00000000', 'TLS00000001'] } } )
    for sample in from_samples_200519_1510:
        assert sample[CREATED_DATE_FIELD_NAME] == '2020-05-19 15:10:00 UTC'

    from_samples_200520_1510 = db.samples.find( { 'Root Sample ID': { '$in': ['TLS00000002', 'TLS00000003'] } } )
    for sample in from_samples_200520_1510:
        assert sample[CREATED_DATE_FIELD_NAME] == '2020-05-20 15:10:00 UTC'

# TODO: include other file name formats - samples_07052020_1610 & tmp_samples_200709_1710 ? DDMMYYYY

def test_query(mongo_database):
  _, db = mongo_database
  collection_name = 'samples_200519_1510'

  db[collection_name].insert_many(generate_example_samples(range(0, 4)))
  db.samples.insert_many(generate_example_samples(range(2, 4)))

  query_samples(db, collection_name)