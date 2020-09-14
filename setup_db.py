from crawler import db

print("Hi I'm being run from Github actions?")

db.init_warehouse_db_command()

print('Finished running that')