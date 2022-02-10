#! /bin/sh
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.3 spark_kafka_consumer_mongo.py

