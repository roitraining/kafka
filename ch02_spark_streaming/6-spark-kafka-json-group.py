#!/usr/bin/python3
# Not working because of a driver issue

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 4-spark-kafka-avro-group.py
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
sc, spark, config = initspark()

df: DataFrame = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .option("startingOffsets", "earliest")
    .load()
    )

# extract the binary value of the message and convert it to the schema read from the avsc file
df1 = df.withColumn('value', from_avro("value", stock_schema))
# flatten out the value struct and remove it
df2 = df1.select(*df.columns, col("value.*")).drop("value")

# pick the columns we want to write to sql
df3 = df2.selectExpr("key as kafka_key", "timestamp as kafka_timestamp", "event_time", "symbol", "price", "quantity")



#df4 = df3.select("symbol","quantity").groupBy(window("symbol", "10 seconds")).sum("quantity")

df4 = (df3.select("kafka_timestamp", "symbol","quantity")
        .withWatermark("kafka_timestamp", "10 seconds") 
        .groupBy(window("kafka_timestamp", "10 seconds"), "symbol")
        .agg(sum("quantity").alias("sum"))
        )
print(df4)


df4 = (df3.select("kafka_timestamp", "symbol","quantity")
        .withWatermark("kafka_timestamp", "10 seconds") 
        .groupBy(window("kafka_timestamp", "10 seconds")) #.alias("group"))
        .agg(sum("quantity").alias("sum"))
        )
print(df4)


def write_memory(df):
    query = (df.writeStream 
            .format("memory")
            .queryName("debug")
            .outputMode("complete")
            .start()
            )
    return query

query = write_console(df3)
query.start().awaitTermination()

writeStream
    .format("memory")
    .queryName("tableName")
    .start()


df4.writeStream.outputMode("append").format("console").start().awaitTermination()
# alternatively, use spark sql
# df2.createOrReplaceTempView('trades')
# df3 = spark.sql("""
# SELECT key as kafka_key, timestamp as kafka_timestamp, event_time, symbol, price
# FROM trades
# """)

# def foreach_batch_function(df, epoch_id):
#     print('foreach_batch')
#     cnt = df.count()
#     if cnt > 0:
#         print('count:', cnt)
#         datafile.writeStream.outputMode("append").format("console").start()

# query = df3.writeStream.foreachBatch(foreach_batch_function)
# query.start().awaitTermination()

# df3.writeStream.outputMode("append").format("console").start().awaitTermination()

# val resultDF = initDF.select("Name", "Date", "Open", "High", "Low")
#   .groupBy(window($"Date", "10 days"), $"Name")
#   .agg(max("High").as("Max"))
#   .orderBy($"window.start")

