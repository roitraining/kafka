#!/usr/bin/python3
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark-kafka-json-consumer.py
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 

# ,org.apache.spark:spark-avro_2.12:3.2.1
# https://sparkbyexamples.com/spark/spark-streaming-consume-and-produce-kafka-messages-in-avro-format/

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
from initspark import initspark
sc, spark, config = initspark()

df: DataFrame = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", brokers) 
    .option("subscribe", kafka_topic) 
#    .option("startingOffsets", "earliest")
    .load()
    )

# df.createOrReplaceTempView('table')
# df1 = spark.sql("""SELECT 'new data', * from table""")
df1 = df.selectExpr("UPPER(CAST(value AS STRING))")




# df2 = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

df2 = (df1.writeStream.format("kafka")
          .option("kafka.bootstrap.servers", brokers) 
          .option("topic","classroom")
          .option("checkpointLocation", "/tmp")
          .start()
        )
df2.awaitTermination()

#df1.writeStream.outputMode("append").format("console").start()




#print(type(df), type(df1))

# df2 = df.writeStream.queryName("df2").format("memory").start()
# df3 = spark.sql("select * from df2")
# df3.show()

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


"""
import os, sys, json
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
sys.path.append('/class')

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Kafka variables
brokers = 'localhost:9092'
kafka_topic = 'stocks-json'
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

"""
