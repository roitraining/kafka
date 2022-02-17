#!/usr/bin/python3
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 spark-kafka-avro-consumer.py

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
from pyspark.sql import DataFrame

# Kafka variables
brokers = 'localhost:9092'
kafka_topic = 'avro-stocks'
receiver_sleep_time = 4
stock_schema = avro.schema.parse(open("stock.avsc", "rb").read())

# Connect to Spark 
# sc = SparkContext(master = 'local[*]', appName = 'test')
from initspark import initspark
sc, spark, config = initspark()

df: DataFrame = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
    .option("startingOffsets", "earliest")
    .load()
    )
#df1 = df.selectExpr("CAST(value AS STRING)")
#print(type(df), type(df1))

df2 = df.writeStream.queryName("df2").format("memory").start()
df3 = spark.sql("select * from df2")
df3.show()

#print(type(df), dir(df))

#df2 = df.writeStream.format("console").start()

#df3 = df.foreach(print).writeStream.format("console").start()

#df2.awaitTermination()
#df3.awaitTermination()

#df2: DataFrame = df.foreach(print) 
#.writeStream.format("console").start()
#print(type(df2), dir(df2), dir(df2.collect))

#df2.awaitTermination()

#stocks.foreachRDD(lambda rdd: rdd_to_df(rdd))

#print(df.take(5))
# df2 = df.select(from_avro("value", stock_schema).alias("value"))
# df2.show(5)


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


#https://mtpatter.github.io/bilao/notebooks/html/01-spark-struct-stream-kafka.html