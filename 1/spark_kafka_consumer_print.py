#!/usr/bin/python3

# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 spark_kafka_consumer_print.py


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

receiver_sleep_time = 4

sc = SparkContext(master = 'local[*]', appName = 'test')
ssc = StreamingContext(sc, batchDuration = receiver_sleep_time)
brokers = 'localhost:9092'
kafka_topic = 'stocks'

stocks = KafkaUtils.createDirectStream(ssc
    , [kafka_topic]
    , kafkaParams={"metadata.broker.list" : brokers})
stocks.pprint()

ssc.start()
ssc.awaitTermination()
