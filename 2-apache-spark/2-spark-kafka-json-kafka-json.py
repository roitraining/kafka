#! /usr/bin/python3
"""
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 2-spark-kafka-json-kafka-json.py

This example will read the stream stocks-json, and just do a minor uppercase transform 
on the data and republish them as new messages to the kafka stream classroom.

Monitor that output by running;
classroom-consumer.py
"""

import os, sys, json, io
from pyspark.sql import *
from pyspark.sql.utils import StreamingQueryException
import sys
import json

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
sys.path.append('/class')

# Kafka variables
brokers = 'localhost:9092'
kafka_topic = 'stocks-json'
receiver_sleep_time = 4

# Connect to Spark 
if not 'sc' in locals():
  from initspark import initspark
  sc, spark, config = initspark()

def publish_to_kafka(df, brokers, topic):
    query = (df1.writeStream.format("kafka")
              .option("kafka.bootstrap.servers", brokers) 
              .option("topic", topic)
              .option("checkpointLocation", "/tmp")
            )
    return query            

df = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .option("startingOffsets", "earliest")
    .load()
    )
print('df', df)

# df.createOrReplaceTempView('table')
# df1 = spark.sql("""SELECT 'new data' as newfield, * from table""")

df1 = df.selectExpr("UPPER(CAST(value AS STRING)) as value")
print('df1', df1)

query = publish_to_kafka(df1, brokers, 'classroom')
query.start().awaitTermination()