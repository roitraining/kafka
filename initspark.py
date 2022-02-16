# you should make sure you have spark in your python path as below
# export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
# but if you don't it will append it automatically for this session

import platform, os, sys
from os.path import dirname

if not 'SPARK_HOME' in os.environ and not os.environ['SPARK_HOME'] in sys.path:
    sys.path.append(os.environ['SPARK_HOME']+'/python')

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

packages = ['org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2'
           ,'org.mongodb.spark:mongo-spark-connector_2.11:2.4.3'
           ,'com.datastax.spark:spark-cassandra-connector_2.11:2.5.2'
           ]

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages ' + ','.join(packages) + ' pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,com.datastax.spark:spark-cassandra-connector_2.11:2.4.0 pyspark-shell'

def initspark(appname = "Test", servername = "local"
    , cassandra = "127.0.0.1", cassandra_user = 'cassandra', cassandra_password='student'
    , mongo = "mongodb://127.0.0.1", mongo_user = '', mongo_password = ''):
    print ('initializing pyspark')
    conf = (SparkConf().set("spark.cassandra.connection.host", cassandra)
            .setAppName(appname)
            .setMaster(servername)
            .set("spark.cassandra.auth.username", cassandra_user) 
            .set("spark.cassandra.auth.password", cassandra_password) 
    )
    # print(f'Cassandra {cassandra} user: {cassandra_user} pw: {cassandra_password}')
    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName(appname) \
    .config("spark.mongodb.input.uri", mongo) \
    .config("spark.mongodb.output.uri", mongo) \
    .enableHiveSupport().getOrCreate()
    sc.setLogLevel("WARN")
    print ('pyspark initialized')
    return sc, spark, conf

def display(df, limit = 10):
    from IPython.display import display    
    display(df.limit(limit).toPandas())

if __name__ == '__main__':
    sc, spark, conf = initspark()

