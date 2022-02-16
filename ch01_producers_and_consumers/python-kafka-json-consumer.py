#! /usr/bin/python3

from kafka import KafkaConsumer
consumer = KafkaConsumer('json-stocks')
print("consumer = ", consumer)
for event in consumer:
   print(event)
