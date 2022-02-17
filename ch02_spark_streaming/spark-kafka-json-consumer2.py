#!/usr/bin/python3

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark-kafka-json-consumer2.py

import os, sys, json
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
sys.path.append('/class')

# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils

# Kafka variables
brokers = 'localhost:9092'
kafka_topic = 'avro-stocks'
receiver_sleep_time = 4

# Connect to Spark 
# sc = SparkContext(master = 'local[*]', appName = 'test')
from initspark import initspark
sc, spark, config = initspark()

def rdd_to_df(rdd):
    try:
        df = rdd.toDF(schema=['event_time', 'symbol', 'price', 'quantity'])
        print(df.collect())
    except Exception as e:
        print(e)
        return
    
stocks = (spark.readStream
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .load()
    )
#    .option("startingOffsets", "earliest")
print("stocks", stocks)

stocks1 = stocks.selectExpr("CAST(value as STRING)").alias("valueString")
stocks2 = stocks1.writeStream.outputMode("append").format("console").start().awaitTermination()
print("stocks2", stocks2)
print(dir(stocks2))
        # KafkaUtils.createDirectStream(ssc, [kafka_topic]
        #     , kafkaParams={"metadata.broker.list" : brokers})
        # .map(lambda x:json.loads(x[1]))
        # )



# stocks.foreachRDD(lambda rdd: rdd_to_df(rdd))

# ssc.start()
# ssc.awaitTermination()

#pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --master local[*]
# https://www.linkedin.com/pulse/pyspark-structured-streaming-kafka-insightglobal-laurent-weichberger/
