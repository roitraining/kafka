# pip install pymongo
# pyspark --packages org.mongodb.spark:mongo-spark-connector_2.12:2.4.3
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:2.4.3 spark-mongo-test.py

import pymongo
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
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

import sys
sys.path.append('/class')
from initspark import initspark
sc, spark, conf = initspark(mongo =  "mongodb://127.0.0.1/classroom")

# from pyspark import SparkContext
# from pyspark.sql import SparkSession

# sc = SparkContext()
# spark = SparkSession.builder.appName("myApp")\
#     .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/classroom") \
#     .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/classroom") \
#     .getOrCreate()

print ('*' * 80)
print ('read through spark and append records')
print ('*' * 80)
df = spark.read.format("mongo").option("uri", "mongodb://127.0.0.1/classroom.people").load()
print ('*' * 80)
print ('from pyspark')
df.show()


new_data = [{'personid':40, 'firstname':'Sri'}
                 , {'personid':50, 'firstname': 'Han'}
                 ]
x1 = spark.createDataFrame(new_data)

x1.write.format("mongo").option("uri", "mongodb://127.0.0.1/classroom.people").options(collection="people", database="classroom").mode("append").save()

print ('*' * 80)
print ('re-read through spark after spark append records')
print ('*' * 80)
df = spark.read.format("mongo").option("uri", "mongodb://127.0.0.1/classroom.people").load()
df.show()


print ('*' * 80)
print ('sparksql example')
print ('*' * 80)
df.createOrReplaceTempView('people')
spark.sql('select personid, UPPER(firstname) as name from people order by name').show()
print ('*' * 80)

