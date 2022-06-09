#! /usr/bin/python3
"""
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 1-spark-kafka-json-console.py
spark-submit --jars spark-sql-kafka.jar 1-spark-kafka-json-console.py

This example will read the stream stocks-json, and just do a minor uppercase transform
on the data and display it to the console.
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

def write_console(df):
    query = (df.writeStream 
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        )
    return query

df = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", False)
    .load()
    )

df.createOrReplaceTempView('table')
#df1 = spark.sql("""SELECT 'new data' as newfield, * from table""")
df1 = df.selectExpr("UPPER(CAST(value AS STRING)) as value")

query = write_console(df1)
query.start().awaitTermination()


