# launch pyspark with the following command
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 1-pyspark-kafka-json-consumer.py
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 

# paste these commands into the pyspark window
import os, sys, json, io
from pyspark.sql import *
from pyspark.sql.utils import StreamingQueryException
import sys
import json

brokers = 'localhost:9092'
kafka_topic = 'stocks-json'
receiver_sleep_time = 4

# Re
df = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .load()
    )
df1 = df.selectExpr("UPPER(CAST(value AS STRING)) as value")

df2 = (df1.writeStream 
    .outputMode("append")
    .format("console")
    .option("truncate", false)
    .start()
    .awaitTermination()
