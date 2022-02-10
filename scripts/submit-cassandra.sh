#! /bin/sh
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 --conf spark.cassandra.connection.host=127.0.0.1 cassandra-pyspark.py

