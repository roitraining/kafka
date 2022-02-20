#! /usr/bin/python3
"""
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 2b-spark-kafka-json-kafka-avro.py
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1

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
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import *


os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
sys.path.append('/class')

# Kafka variables
brokers = 'localhost:9092'
kafka_topic = 'stocks-json'
receiver_sleep_time = 4
stock_schema = open("stock.avsc", "r").read()

# Connect to Spark 
from initspark import initspark
sc, spark, config = initspark()
#spark.sparkContext.setLogLevel("ERROR")

stock_struct = spark.read.format("avro").option("avroSchema", stock_schema).load().schema
print(stock_schema)

df = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", False)
    .load()
    )
print(df)

# df.createOrReplaceTempView('table')
# df1 = spark.sql("""SELECT 'new data' as newfield, * from table""")
#df1 = df.selectExpr("from_json(CAST(value AS STRING), stock_schema) as value")


# cast the binary json to a string
df1 = df.selectExpr("key", "CAST(value AS STRING) as value")
print('df1', df1)

# cast the string json to a struct
df2 = df1.select(*df1.columns, from_json(df1.value, stock_struct).alias("value2")).drop('value')
print('df2', df2)

# flatten the struct to a normal DataFrame
df3 = df2.select(*(df2.columns), col("value2.*")).drop('value2')
print('df3', df3)

# df3 = df2.withColumn('value', to_avro("value", stock_schema))
# print('df2', df2)

df3.createOrReplaceTempView("data")
df4 = spark.sql("""
SELECT key, cast(NAMED_STRUCT('event_time', event_time, 'symbol', symbol, 'price', price, 'quantity', quantity) as string) AS value 
FROM data
""")
print('df4', df4)

df5 = df4.selectExpr("key", to_avro("value", stock_schema))
print('df5', df5)

# query = (df1.writeStream 
#     .outputMode("append")
#     .format("console")
#     .option("truncate", False)
#     .start()
#     )

# query.awaitTermination()

query = (df5.writeStream.format("kafka")
          .option("kafka.bootstrap.servers", brokers) 
          .option("topic","stocks-avro")
          .option("checkpointLocation", "/tmp")
          .start()
        )
query.awaitTermination()




