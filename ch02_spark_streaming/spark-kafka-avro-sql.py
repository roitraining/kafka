#!/usr/bin/python3
# Not working because of a driver issue

# spark-submit --jars /usr/share/java/mysql-connector-java.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 spark-kafka-avro-consumer.py
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
df3 = df2.selectExpr("key as kafka_key", "timestamp as kafka_timestamp", "event_time", "symbol", "price")

# alternatively, use spark sql
# df2.createOrReplaceTempView('trades')
# df3 = spark.sql("""
# SELECT key as kafka_key, timestamp as kafka_timestamp, event_time, symbol, price
# FROM trades
# """)

mysql_url = "jdbc:mysql://localhost:3306/stocks"
# mysql_table             
mysql_login = {
     "user": "python",
     "password": "student"
     }

def foreach_batch_function(df, epoch_id):
    mysql_url="jdbc:mysql://localhost:3306/stocks?user=python&password=student"
    df.write.jdbc(mysql_url, table = 'trades')

query = df3.writeStream.foreachBatch(foreach_batch_function)
query.start().awaitTermination()
