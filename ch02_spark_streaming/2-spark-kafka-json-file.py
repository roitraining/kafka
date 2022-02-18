#! /usr/bin/python3
"""
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 2-spark-kafka-json-file.py

This example will read the stream stocks-json, and just do a minor uppercase transform on the data
and republish them as new messages to the kafka stream classroom.

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
from initspark import initspark
sc, spark, config = initspark()
#spark.sparkContext.setLogLevel("ERROR")

df: DataFrame = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", False)
    .load()
    )

# df.createOrReplaceTempView('table')
# df1 = spark.sql("""SELECT 'new data' as newfield, * from table""")
df1 = df.selectExpr("UPPER(CAST(value AS STRING)) as value")

# query = (df1
#     .writeStream
#     .outputMode("append") 
#     .format("csv") 
#     .option("path", "output")
#     .option("header", True)
#     .option("checkpointLocation", "/tmp")
#     .start()
#   )
# query.awaitTermination()


query = (df1.writeStream.format("kafka")
          .option("kafka.bootstrap.servers", brokers) 
          .option("topic","classroom")
          .option("checkpointLocation", "/tmp")
          .start()
        )
query.awaitTermination()

