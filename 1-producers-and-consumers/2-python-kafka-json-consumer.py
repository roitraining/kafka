#! /usr/bin/python3

import json
from kafka import KafkaConsumer
consumer = KafkaConsumer('stocks-json')
print("consumer = ", consumer)
for event in consumer:
   #print('json consumer -', event)
   key = event.key
   value = event.value
   value2 = json.loads(value)
   #print(type(value), type(value2), value2)
   print(value2['symbol'], value2['quantity'])