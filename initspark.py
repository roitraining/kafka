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
from pyspark.sql.functions import *

# packages = [
#            'com.datastax.spark:spark-cassandra-connector_2.12:3.0.0'
#            ,'org.mongodb.spark:mongo-spark-connector_2.12:2.4.3'
#            ,'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2'
#            ,'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1'
#            ,'org.apache.spark:spark-avro_2.12:3.2.1'
#            ]


# packages = [
#            'org.mongodb.spark:mongo-spark-connector_2.12:2.4.3'
#            ]


# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages ' + ','.join(packages) + ' pyspark-shell'
# #os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,com.datastax.spark:spark-cassandra-connector_2.11:2.4.0 pyspark-shell'
# print(os.environ['PYSPARK_SUBMIT_ARGS'])
# # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages ' + 'com.datastax.spark:spark-cassandra-connector_2.12:3.0.0' + ' pyspark-shell'
# # print(os.environ['PYSPARK_SUBMIT_ARGS'])

# org.apache.spark/spark-streaming-kafka-0-10_2.12 "3.2.1"
# org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1

def initspark(appname = "Test", servername = "local"
    , cassandra = "127.0.0.1", cassandra_user = 'cassandra', cassandra_password='student'
    , mongo = "mongodb://127.0.0.1", mongo_user = '', mongo_password = '', packages = None):

    print ('initializing pyspark')
    package_list = {
            'cassandra' : 'com.datastax.spark:spark-cassandra-connector_2.12:3.0.0'
            , 'mongo' : 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.3'
#            , 'kafka' : 'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2'
            , 'kafka' : 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1'
            , 'kafka-sql' : 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1'
            , 'spark-avro' : 'org.apache.spark:spark-avro_2.12:3.2.1'
            , 'hbase' : 'com.hortonworks:shc-core:1.1.1-2.1-s_2.11'
#            , 'hbase' : 'org.apache.hbase.connectors.spark:hbase-spark:1.0.0'
            }

    print('packages', packages)
    os.environ['PYSPARK_SUBMIT_ARGS'] = "pyspark-shell"

    if packages:
        if type(packages) is str and packages.lower() == 'all':
            print("All Packages")
            os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages ' + ','.join(package_list.values()) + ' pyspark-shell'
            print(os.environ['PYSPARK_SUBMIT_ARGS'])
        elif type(packages) is list:
            p = [p for n,p in package_list.items() if n in packages]
            os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages ' + ','.join(p) + ' pyspark-shell'
            print(os.environ['PYSPARK_SUBMIT_ARGS'])
    else:
        print('No pacakges chosen', os.environ['PYSPARK_SUBMIT_ARGS'])

    if 'cassandra' in os.environ['PYSPARK_SUBMIT_ARGS']:
        conf = (SparkConf().set("spark.cassandra.connection.host", cassandra)
                .setAppName(appname)
                .setMaster(servername)
                .set("spark.cassandra.auth.username", cassandra_user) 
                .set("spark.cassandra.auth.password", cassandra_password) 
            )
    else:
        conf = (SparkConf()
            .setAppName(appname)
            .setMaster(servername)
        )

    # print(f'Cassandra {cassandra} user: {cassandra_user} pw: {cassandra_password}')
    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")
    spark = (SparkSession.builder.appName(appname) 
    .config("spark.mongodb.input.uri", mongo) 
    .config("spark.mongodb.output.uri", mongo) 
    .config("spark.jars", "/usr/share/java/mysql-connector-java.jar")
    .enableHiveSupport().getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print ('pyspark initialized')
    return sc, spark, conf

def display(df, limit = 10):
    from IPython.display import display    
    display(df.limit(limit).toPandas())

if __name__ == '__main__':
    sc, spark, conf = initspark()

