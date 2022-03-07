import apache_beam as beam
from apache_beam.io.mongodbio import ReadFromMongoDB, WriteToMongoDB

MONGO_URI = 'mongodb://localhost:27017'

import pymongo
client = pymongo.MongoClient(MONGO_URI)
classroom = client["classroom"]
if 'classroom' in (x['name'] for x in client.list_databases()):
    client.drop_database('classroom')

people = classroom['people']
name = {"personid" : 10, "firstname" : "Adam"}
x = people.insert_one(name)

names = [{"personid" : 20, "firstname" : "Betty"}
         ,{"personid" : 30, "firstname" : "Charlie"}]
x = people.insert_many(names)

print ('*' * 80)
print ('from mongo directly')
x = people.find()
print (list(x))
print ('*' * 80)

print ('*' * 80)
print ('read through beam and append records')
print ('*' * 80)

new_data = [{'personid':40, 'firstname':'Sri'}
                 , {'personid':50, 'firstname': 'Han'}
                 ]

with beam.Pipeline() as p:
    (p
    | 'Read 1' >> ReadFromMongoDB(uri = MONGO_URI, db='classroom', coll='people')
    | 'Print 1' >> beam.Map(print)
    )

    (p
    | beam.Create(new_data)
    | WriteToMongoDB(uri = MONGO_URI, db='classroom', coll='people')
    )

print ('*' * 80)
print ('re-read through beam after append')
print ('*' * 80)
with beam.Pipeline() as p:
    (p
    | 'Read 2' >> ReadFromMongoDB(uri = MONGO_URI, db='classroom', coll='people')
    | 'Print 2' >> beam.Map(print)
    )

print ('*' * 80)
print ('from mongo directly')
x = people.find()
print (list(x))
print ('*' * 80)

    