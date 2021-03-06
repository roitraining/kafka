import pymongo

client = pymongo.MongoClient("mongodb://mongodb:27017/")
print("client = ", client)

classroom = client["classroom"]

if 'classroom' in (x['name'] for x in client.list_databases()):
    client.drop_database('classroom')

people = classroom['people']

name = {"firstname" : "Adam", "personid":4}

x = people.insert_one(name)

names = [{"firstname" : "Betty", "personid":5}
         ,{"firstname" : "Charlie", "personid":6}]

x = people.insert_many(names)

x = people.find()

print ('*' * 80)
print ('from mongo directly')
print (list(x))
print ('*' * 80)

from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession.builder.appName("myApp")\
    .config("spark.mongodb.input.uri", "mongodb://mongodb/classroom") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb/classroom") \
    .getOrCreate()

df = spark.read.format("mongo").option("uri", "mongodb://mongodb/classroom.people").load()
print ('*' * 80)
print ('from pyspark')
df.show()
print ('*' * 80)

x = sc.parallelize([(7, 'David')])
x1 = spark.createDataFrame(x, schema = ['personid', 'firstname'])
x1.write.format("mongo").options(collection="people", database="classroom").mode("append").save()

print ('*' * 80)
print ('from pyspark after insert')
df = spark.read.format("mongo").option("uri", "mongodb://mongodb/classroom.people").load()
df.show()
print ('*' * 80)

df.createOrReplaceTempView('people')

print ('*' * 80)
print ('spark sql')
spark.sql('select * from people').show()
print ('*' * 80)
