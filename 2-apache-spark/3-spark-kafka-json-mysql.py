#! /usr/bin/python3
"""
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 3-spark-kafka-json-mysql.py

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
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import *

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

stock_schema = open("stock.avsc", "r").read()
print('stock_schema', stock_schema)
stock_struct = spark.read.format("avro").option("avroSchema", stock_schema).load().schema
print('stock_struct', stock_struct)

df = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", False)
    .load()
    )
print('df', df)

# df.createOrReplaceTempView('table')
# df1 = spark.sql("""SELECT 'new data' as newfield, * from table""")

df1 = df.selectExpr("CAST(value AS STRING) as value")
print('df1', df1)

# cast the string json to a struct
df2 = df1.select(*df1.columns, from_json(df1.value, stock_struct).alias("value2")).drop('value')
print('df2', df2)

# flatten the struct to a normal DataFrame
df3 = df2.select(*(df2.columns), col("value2.*")).drop('value2')
print('df3', df3)

df4 = df3.withColumnRenamed('key','kafka_key').withColumnRenamed('timestamp', 'kafka_timestamp')
print('df4', df4)

def foreach_batch_to_sql(df, epoch_id):
    cnt = df.count()
    print(f'foreach_batch cnt = {cnt}')
    mysql_url = "jdbc:mysql://127.0.0.1:3306/stocks"

    # mysql_table             
    mysql_login = {
        "user": "python",
        "password": "student"
        }

    if cnt > 0:
        print('count:', cnt)
        mysql_url="jdbc:mysql://localhost:3306/stocks?user=python&password=python"
        df.write.mode('append').jdbc(mysql_url, table = 'trades') #.save()

query = df4.writeStream.foreachBatch(foreach_batch_to_sql)
query.start().awaitTermination()

def write_console(df):
    query = (df.writeStream 
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            )
    return query

# query = write_console(df3)
# query.start().awaitTermination()

def publish_to_kafka(df, brokers, topic):
    query = (df1.writeStream.format("kafka")
              .option("kafka.bootstrap.servers", brokers) 
              .option("topic", topic)
              .option("checkpointLocation", "/tmp")
            )
    return query            

# query = publish_to_kafka(df1, brokers, 'classroom')
# query.start().awaitTermination()