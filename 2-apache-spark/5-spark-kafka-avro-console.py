#! /usr/bin/python3
"""
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 5-spark-kafka-avro-console.py

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
import uuid
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import *

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
sys.path.append('/class')

# Kafka variables
brokers = 'localhost:9092'
kafka_topic = 'stocks-avro'
receiver_sleep_time = 4

# Connect to Spark 
if not 'sc' in locals():
  from initspark import initspark
  sc, spark, config = initspark()

stock_schema = open("stock.avsc", "r").read()
print('stock_schema', stock_schema)
stock_struct = spark.read.format("avro").option("avroSchema", stock_schema).load().schema
print('stock_struct', stock_struct)

# def avro_to_dict(msg):
#    buf = io.BytesIO()
#    buf.seek(0)
#    buf.write(msg)
#    x1 = avro.datafile.DataFileReader(buf, avro.io.DatumReader())
#    x2 = next(x1)
#    return x2


df = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", False)
    .load()
    )
print('df', df)

# df.createOrReplaceTempView('table')
# df1 = spark.sql("""SELECT 'new data' as newfield, * from table""")
#df1 = df.select(col("value").alias('value'))
#df1 = df.select(col("value").alias("old"), from_avro(col("value"), stock_schema, options = {"mode":"PERMISSIVE"}).alias('value'))
#df1 = df.withColumn("value", col("value").cast(StringType()))
# df1 = df.withColumn("value", col("value").cast(StringType()))
# print('df1', df1)
# df2 = df1.select(*df1.columns, from_json(df1.value, stock_struct).alias("value2")).drop("value")
# print('df2', df2)

#df1 = df.select("key", avro_to_dict(df.value).alias("value"))

df2 = df.select("key", from_avro(df.value, stock_schema, options = {"mode":"PERMISSIVE"}).alias("value"))
print('df2', df2)

# flatten the struct to a normal DataFrame
df3 = df2.select(*(df2.columns), col("value.*")).drop("value")
print('df3', df3)

#df4 = df3.select(uuid.UUID(bytes=df3.key))

#df4 = df3.select(uuid.UUID(bytes=df3.key).alias("key2"), *(df2.columns))

'''
# cast the string json to a struct
df2 = df1.select(*df1.columns, from_json(df1.value, stock_struct).alias("value2")).drop('value')
print('df2', df2)

# flatten the struct to a normal DataFrame
df3 = df2.select(*(df2.columns), col("value2.*")).drop('value2')
print('df3', df3)

df3.createOrReplaceTempView('data')
df4 = spark.sql("""
SELECT NAMED_STRUCT('event_time', event_time, 'symbol', symbol, 'price', price, 'quantity', quantity) AS value
FROM data
""")
print('df4', df4)

df5 = df4.select(to_json("value").alias("value"))
print('df5', df5)

df6 = df5.select(to_avro("value").alias("value"))
print('df6', df6)
'''

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

# query = df3.writeStream.foreachBatch(foreach_batch_to_sql)
# query.start().awaitTermination()

def write_console(df):
    query = (df.writeStream 
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            )
    return query

query = write_console(df3)
query.start().awaitTermination()

def publish_to_kafka(df, brokers, topic):
    query = (df1.writeStream.format("kafka")
              .option("kafka.bootstrap.servers", brokers) 
              .option("topic", topic)
              .option("checkpointLocation", "/tmp")
            )
    return query            

# query = publish_to_kafka(df6, brokers, 'stocks-avro')
# query.start().awaitTermination()
#df1 DataFrame[value2: struct<event_time:string,symbol:string,price:float,quantity:int>]