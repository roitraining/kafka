#!/usr/bin/python3
# Not working because of a driver issue
# spark-submit --jars /usr/share/java/mysql-connector-java.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 spark-kafka-avro-consumer.py
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1

# spark-submit --jars /usr/share/java/mysql-connector-java.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 spark-kafka-avro-consumer.py
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1,org.apache.bahir:spark-sql-streaming-jdbc_2.12:2.4.0 spark-kafka-avro-consumer.py

# pyspark --jars /usr/share/java/mysql-connector-java.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1
# pyspark --packages org.apache.bahir:spark-sql-streaming-jdbc_2.12:2.4.0
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
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Kafka variables
brokers = 'localhost:9092'
kafka_topic = 'avro-stocks'
receiver_sleep_time = 4
#stock_schema = avro.schema.parse(open("stock.avsc", "rb").read())
stock_schema = open("stock.avsc", "r").read()

#stock_struct = StructType.fromJson(stock_schema.to_json())
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

# extract the binary value of the message and convert it to the schema read from the avsc file
df1 = df.withColumn('value', from_avro("value", stock_schema))
# flatten out the value struct and remove it
df2 = df1.select(*df.columns, col("value.*")).drop("value")

df3 = df2.selectExpr("key as kafka_key", "timestamp as kafka_timestamp", "event_time", "symbol", "price")

# df2.createOrReplaceTempView('trades')
# df3 = spark.sql("""
# SELECT key as kafka_key, timestamp as kafka_timestamp, event_time, symbol, price
# FROM trades
# """)

mysql_url = "jdbc:mysql://localhost:3306/stocks"
# mysql_table             
mysql_login = {
     "user": "python",
     "password": "student"
     }
mysql_info = {"url": "jdbc:mysql://localhost:3306/stocks"
            , "table": "trades"
            , "properties": mysql_login
            , "mode": "append"
#            , "driver": "com.mysql.jdbc.Driver"
            }
# 

mysql_info = {"url": "jdbc:mysql://localhost:3306/stocks"
            , "table": "trades"
            , "mode": "append"
            , "driver": "com.mysql.jdbc.Driver"
            ,  "user": "python"
            ,  "password": "student"
            }



def foreach_batch_function(df, epoch_id):
    mysql_url="jdbc:mysql://localhost:3306/stocks?user=python&password=student"
    df.write.jdbc(mysql_url, table = 'trades')
    # df.write.jdbc(url = mysql_info['url'], table = mysql_info['table']
    #         , driver = mysql_info['driver']
    #         , mode = "append", properties = mysql_login)
    # print('foreach_batch_function', epoch_id, df)
    # cnt = df.count()
    # df.show()
    # if cnt <= 0:
    #     print('None')
    # else:
    #     print(cnt)
    #     df.write.format("jdbc").options(**mysql_info).save()

query = df3.writeStream.foreachBatch(foreach_batch_function)
query.start().awaitTermination()


# def foreach_batch_function(df, epoch_id):
#     print('foreach_batch_function', epoch_id, df)
#     cnt = df.count()
#     if cnt <= 0:
#         print('None')
#     else:
#         print(cnt)
#         df.write.jdbc(url)
#         df.write.format("jdbc").options(**mysql_info).save()
  
# query = df3.writeStream.foreachBatch(foreach_batch_function)
# query.start().awaitTermination()



# def process_row(df, epoch_id):
#     df2.write.jdbc(url=db_target_url, table="mytopic", mode="append", properties=db_target_properties)
#     pass
# query = df2.writeStream.foreachBatch(process_row).start()



# republish to kafka
# query2 = (df3.writeStream.format("kafka")
#           .option("kafka.bootstrap.servers", brokers) 
#           .option("topic","classroom")
#           .option("checkpointLocation", "/tmp")
#           .start()
#         )


# query = (df.writeStream
#     .format("streaming-jdbc")
#     .option("checkpointLocation", "/tmp")
#     .outputMode("Append")
#     .options(**mysql_info)
# #    .trigger(Trigger.ProcessingTime("10 seconds"))
#     .start()
#     )

# query.awaitTermination()


# # My SQL command to determine port: SHOW GLOBAL VARIABLES LIKE 'PORT';
# def foreach_batch_function(df, epoch_id):
#     (df3.write().jdbc(
#         url = "jdbc:mysql://localhost:3306/stocks"
#         , table = "trades"
#         , user = "python"
#         , password = "student"
#     ))
#     pass

#     #     , 
#     # )
#     #     .format("jdbc")
#     #     .option("url","jdbc:mysql://localhost:3306/stocks?characterEncoding=UTF-8")
#     #     .option("dbtable","trades")
#     #     .option("user","python")
#     #     .option("password","student")
#     #     .mode(SaveMode.Append)
#     #     .save()
#     # )

#     # df.write.jdbc(url='jdbc:mysql://172.16.23.27:30038/securedb',  table="sparkkafka",
#     #   properties=db_target_properties)
#     # pass

# query = df3.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start()

# query.awaitTermination()

# # stocks.foreachRDD(lambda rdd: rdd_to_df(rdd))







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

data =['103', 'tester_1']

df = sc.parallelize(data).toDF(['id', 'name'])

df.write.format('jdbc').options(
      url='jdbc:mysql://localhost/database_name',
      driver='com.mysql.jdbc.Driver',
      dbtable='DestinationTableName',
      user='your_user_name',
      password='your_password').mode('append').save()


x = sc.parallelize([{"id":1, "name" : "abc"}]).toDF()
mysql_url="jdbc:mysql://localhost:3306/stocks?user=python&password=student"
x.write.jdbc(mysql_url, table = 'test').mode('append')


x.format('jdbc').options(
      url='jdbc:mysql://localhost/database_name',
      driver='com.mysql.jdbc.Driver',
      dbtable='DestinationTableName',
      user='your_user_name',
      password='your_password').mode('append').save()

"""      