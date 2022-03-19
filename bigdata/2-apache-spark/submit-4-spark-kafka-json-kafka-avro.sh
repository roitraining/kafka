#! /bin/sh
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 4-spark-kafka-json-kafka-avro.py

