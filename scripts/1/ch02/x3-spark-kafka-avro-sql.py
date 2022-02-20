#!/usr/bin/python3
# Not working because of a driver issue

# spark-submit --jars /usr/share/java/mysql-connector-java.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 3-spark-kafka-avro-sql.py
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
kafka_topic = 'stocks-avro'
receiver_sleep_time = 4
stock_schema = open("stock.avsc", "r").read()
print(stock_schema)

def write_console(df):
    query = (df.writeStream 
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        )
    return query

if not ('spark' in locals()):
    from initspark import initspark
    sc, spark, config = initspark()
    spark.sparkContext.setLogLevel("ERROR")

df = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", False)
    .option("mode", "DROPMALFORMED")
    .load()
    )
print('df', df)

df0 = df.selectExpr("CAST(value AS STRING) as value")


# extract the binary value of the message and convert it to the schema read from the avsc file
df1 = df.withColumn('value', from_avro("value", stock_schema))
print('df1', df1)

# flatten out the value struct and remove it
df2 = df1.select(*df1.columns, col("value.*")).drop("value")
print('df2', df2)

# pick the columns we want to write to sql
df3 = df2.selectExpr("key as kafka_key", "timestamp as kafka_timestamp", "event_time", "symbol", "price")
print(df3)

# alternatively, use spark sql
# df2.createOrReplaceTempView('trades')
# df3 = spark.sql("""
# SELECT key as kafka_key, timestamp as kafka_timestamp, event_time, symbol, price
# FROM trades
# """)

mysql_url = "jdbc:mysql://127.0.0.1:3306/stocks"
# mysql_table             
mysql_login = {
     "user": "python",
     "password": "student"
     }

def foreach_batch_function(df, epoch_id):
    print('foreach_batch')
    cnt = df.count()
    if cnt > 0:
        print('count:', cnt)
        mysql_url="jdbc:mysql://localhost:3306/stocks?user=python&password=python"
        df.write.mode('append').jdbc(mysql_url, table = 'trades').save()

query = df3.writeStream.foreachBatch(foreach_batch_function)
query.start().awaitTermination()

'''
query = (df1.writeStream 
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("mode", "DROPMALFORMED")
    .start()
    )
query.awaitTermination()
'''


query = write_console(df0)
query.start()
query.stop()


Objavro.codec\bnullavro.schema�{"type": "record", "name": "Stock", "namespace": "example.avro", "fields": [{"type": "string", "name": "event_time"}, {"type": "string", "name": "symbol"}, {"type": "float", "name": "price"}, {"type": "int", "name": "quantity"}]}p@��Fd��8�{ݪJ>&2022-02-18 04:59:01\bMSFT�oC�\bp@��Fd��8�{ݪJ|


|Objavro.codec\bnullavro.schema�{"type": "record", "name": "Stock", "namespace": "stock.avro", "fields": [{"type": "string", "name": "event_time"}, {"type": "string", "name": "symbol"}, {"type": "float", "name": "price"}, {"type": "int", "name": "quantity"}]}R*����S���$>&2022-02-18 05:02:42\bAAPL�0C�R*����S���$    |
