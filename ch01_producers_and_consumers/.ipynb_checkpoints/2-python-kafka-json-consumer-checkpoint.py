#! /usr/bin/python3

from kafka import KafkaConsumer
consumer = KafkaConsumer('stocks-json')
print("consumer = ", consumer)
for event in consumer:
   print(event)
