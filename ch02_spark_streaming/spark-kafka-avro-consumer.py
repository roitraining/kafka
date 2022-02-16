#!/usr/bin/python3

# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 spark-kafka-avro-consumer.py

import os, sys, json, io
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
sys.path.append('/class')

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import fastavro
import avro.io
import avro.schema
import avro.datafile

# Kafka variables
brokers = 'localhost:9092'
kafka_topic = 'stocks'
receiver_sleep_time = 4

# Connect to Spark 
# sc = SparkContext(master = 'local[*]', appName = 'test')
from initspark import initspark
sc, spark, config = initspark()
ssc = StreamingContext(sc, batchDuration = receiver_sleep_time)

stock_schema = avro.schema.parse(open("stock.avsc", "rb").read())

def rdd_to_df(rdd):
    try:
        df = rdd.toDF(schema=['event_time', 'symbol', 'price', 'quantity'])
        print(df.collect())
    except Exception as e:
        print(e)
        return

def avro_decoder(msg):
    stock_schema = avro.schema.parse(open("stock.avsc", "rb").read())

    bytes_io = io.BytesIO(msg)
    bytes_io.seek(0)
    msg_decoded = fastavro.schemaless_reader(bytes_io, schema)
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



                                             