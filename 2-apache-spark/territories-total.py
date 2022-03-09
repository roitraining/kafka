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
    sc, spark, config = initspark(packages = ['kafka', 'kafka-sql', 'spark-avro'])

territories = spark.read.csv('file:///class/2-apache-spark/territories.csv', header=True, inferSchema = True)
territories.createOrReplaceTempView('territories')
t1 = spark.sql("""SELECT regionid, count(*) as cnt 
          from territories 
          where territoryname like '%a%' 
          group by regionid 
          order by cnt desc""")
t1.write.csv('hdfs://localhost:9000/territories_total', sep = '|')
        