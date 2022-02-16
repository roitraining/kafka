#!/usr/bin/python3

# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 spark-kafka-json-consumer.py

import os, sys, json
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
sys.path.append('/class')

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Kafka variables
brokers = 'localhost:9092'
kafka_topic = 'stocks'
receiver_sleep_time = 4

# Connect to Spark 
# sc = SparkContext(master = 'local[*]', appName = 'test')
from initspark import initspark
sc, spark, config = initspark()
ssc = StreamingContext(sc, batchDuration = receiver_sleep_time)

def rdd_to_df(rdd):
    try:
        df = rdd.toDF(schema=['event_time', 'symbol', 'price', 'quantity'])
        print(df.collect())
    except Exception as e:
        print(e)
        return
    
stocks = (
        KafkaUtils.createDirectStream(ssc, [kafka_topic]
            , kafkaParams={"metadata.broker.list" : brokers})
        .map(lambda x:json.loads(x[1]))
        )

stocks.foreachRDD(lambda rdd: rdd_to_df(rdd))

ssc.start()
ssc.awaitTermination()

