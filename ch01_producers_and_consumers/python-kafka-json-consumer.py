#! /usr/bin/python3

from kafka import KafkaConsumer
consumer = KafkaConsumer('stocks')
print("consumer = ", consumer)
for event in consumer:
   print(event)
