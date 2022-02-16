#!/usr/bin/python3
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 spark-kafka-avro-consumer.py

# https://sparkbyexamples.com/spark/spark-streaming-consume-and-produce-kafka-messages-in-avro-format/
# export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH


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

# Kafka variables
brokers = 'localhost:9092'
kafka_topic = 'avro-stocks'
receiver_sleep_time = 4
stock_schema = avro.schema.parse(open("stock.avsc", "rb").read())

# Connect to Spark 
# sc = SparkContext(master = 'local[*]', appName = 'test')
from initspark import initspark
sc, spark, config = initspark()

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("subscribe", kafka_topic) \
    .load()

df2 = df.select(from_avro("value", stock_schema).alias("value"))
df2.show(5)


"""
ssc = StreamingContext(sc, batchDuration = receiver_sleep_time)

stock_schema = avro.schema.parse(open("stock.avsc", "rb").read())
print('Stock Schema:', stock_schema)

def rdd_to_df(rdd):
    try:
        df = rdd.toDF(schema=['event_time', 'symbol', 'price', 'quantity'])
        print(df.collect())
    except Exception as e:
        print(e)
        return

def avro_decoder(msg):

    bytes_io = io.BytesIO(msg)
    bytes_io.seek(0)
    msg_decoded = fastavro.schemaless_reader(bytes_io, stock_schema)
    print(msg_decoded)
    return msg_decoded

stocks = (
        KafkaUtils.createDirectStream(ssc, [kafka_topic]
            , kafkaParams={"metadata.broker.list" : brokers}
            , valueDecoder = avro_decoder)
        )

stocks.foreachRDD(lambda rdd: rdd_to_df(rdd))

ssc.start()
ssc.awaitTermination()



"""                    