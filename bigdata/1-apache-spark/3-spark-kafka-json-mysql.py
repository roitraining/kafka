#! /usr/bin/python3
"""
spark-submit --jars /usr/share/java/mysql-connector-java.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 3-spark-kafka-json-mysql.py

This example will read the stream stocks-json, and demonstrate how to parse the data
the write it to a MySQL database
"""

import os, sys, json, io
from pyspark.sql import *
from pyspark.sql.utils import StreamingQueryException
import sys
import json
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

# Load the Avro schema file
stock_schema = open("stock.avsc", "r").read()
print('stock_schema', stock_schema)

# Convert the Avro schema into a Spark struct object
stock_struct = spark.read.format("avro").option("avroSchema", stock_schema).load().schema
print('stock_struct', stock_struct)

# Read the Kafka stream
df = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", False)
    .load()
    )
print('df', df)

# Cast the bytes of the message.value to a string
df.createOrReplaceTempView('table')
df1 = spark.sql("""SELECT CAST(value AS STRING) as value, 0 as timestamp, key from table""")

#df1 = df.selectExpr("CAST(value AS STRING) as value")
print('df1', df1)

# cast the string json to a struct object using the from_json and struct object created above
df2 = df1.select(*df1.columns, from_json(df1.value, stock_struct).alias("value2")).drop('value')
print('df2', df2)

# flatten the struct to a normal DataFrame
df3 = df2.select(*(df2.columns), col("value2.*")).drop('value2')
print('df3', df3)

# rename some columns to match the MySQL table structure
df4 = df3.withColumnRenamed('key','kafka_key').withColumnRenamed('timestamp', 'kafka_timestamp')
print('df4', df4)

# There are several types of sinks or destinations we can write streaming sources to:
# 1. console
# 2. file
# 3. kafka
# 4. foreach
# 5. memory 
mysql_url = "jdbc:mysql://127.0.0.1:3306/stocks"
mysql_url="jdbc:mysql://localhost:3306/stocks?user=python&password=python"

def foreach_batch_to_sql(df, epoch_id):
    cnt = df.count()
    print(f'foreach_batch cnt = {cnt}')
#    mysql_url = "jdbc:mysql://127.0.0.1:3306/stocks"

    # mysql_table             
    mysql_login = {"user": "python", "password": "student"}

    if cnt > 0:
        print('count:', cnt)
#        mysql_url="jdbc:mysql://localhost:3306/stocks?user=python&password=python"
#        df2 = spark.sql('SELECT * from df join lookup on ')
        df.write.mode('append').jdbc(mysql_url, table = 'trades') #.save()


#lookup = spark.read.jdbc(mysql_url, table = 'lookup') 

query = df4.writeStream.foreachBatch(foreach_batch_to_sql)
query.start().awaitTermination()

