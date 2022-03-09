#! /bin/sh
spark-submit --jars /usr/share/java/mysql-connector-java.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1 3-spark-kafka-json-mysql.py

