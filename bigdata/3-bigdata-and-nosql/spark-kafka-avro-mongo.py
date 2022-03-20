#!/usr/bin/python3
# Not working because of a driver issue

# spark-submit --packages org.apache.spark:spark-avro_2.12:3.2.1,org.mongodb.spark:mongo-spark-connector_2.12:2.4.3,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark-kafka-avro-mongo.py
# spark-submit --jars /usr/share/java/mysql-connector-java.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 spark-kafka-avro-sql.py
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1

import os, sys, json, io
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
sys.path.append('/class')

#from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils
import fastavro
import avro.io
import avro.schema
import avro.datafile
#import spark.sql.avro

from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Kafka variables
brokers = 'localhost:9092'
kafka_topic = 'avro-stocks'
receiver_sleep_time = 4
stock_schema = open("stock.avsc", "r").read()

from initspark import initspark
sc, spark, config = initspark() #packages = ['kafka', 'mongo', 'spark-avro'])

stock_schema = open("stock.avsc", "r").read()
print('stock_schema', stock_schema)
stock_struct = spark.read.format("avro").option("avroSchema", stock_schema).load().schema
print('stock_struct', stock_struct)

df = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .option("startingOffsets", "earliest")
    .load()
    )

df2 = df.select("key", from_avro(df.value, stock_schema, options = {"mode":"PERMISSIVE"}).alias("value"))
print('df2', df2)

# flatten the struct to a normal DataFrame
df3 = df2.select(*(df2.columns), col("value.*")).drop("value")
print('df3', df3)

# # pick the columns we want to write to sql
# df3 = df2.selectExpr("key as kafka_key", "timestamp as kafka_timestamp", "event_time", "symbol", "price")

def write_mongo(df, epoch_id):
    df.write.format("mongo").options(collection="trades", database="stock").mode("append").save()
    pass
    
query = df3.writeStream.foreachBatch(write_mongo).start()
query.awaitTermination()
