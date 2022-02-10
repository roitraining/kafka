#!/usr/bin/python3

# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.3 receiver1.py

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

def rdd_to_mongo(rdd):
    try:
        df = rdd.toDF(schema=['timestamp', 'symbol', 'price'])
        print(df.collect())
        (df.write.format("mongo")
           .options(database="stocks", collection="trades")
           .mode("append")
           .save()
        )
        print('Done')
    except Exception as e:
        print(e)
        return
    
stocks = (
        KafkaUtils.createDirectStream(ssc, [kafka_topic]
            , kafkaParams={"metadata.broker.list" : brokers})
        .map(lambda x:json.loads(x[1]))
        )

stocks.foreachRDD(lambda rdd: rdd_to_mongo(rdd))

ssc.start()
ssc.awaitTermination()
