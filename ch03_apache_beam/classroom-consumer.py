#! /usr/bin/python3
print('waiting for messages in classroom topic')
from kafka import KafkaConsumer
consumer = KafkaConsumer('classroom')
for event in consumer:
   print(event)
