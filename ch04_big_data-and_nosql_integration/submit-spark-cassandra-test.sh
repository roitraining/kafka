#! /bin/sh
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1,com.datastax.spark:spark-cassandra-connector_2.11:2.5.2 spark-cassandra-test.py


