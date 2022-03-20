#! /usr/bin/python3
# start 1-python-kafka-json-producer.py in one terminal window, then start this program in another terminal window 

import json, uuid
from kafka import KafkaConsumer, TopicPartition

# Consumer starting from default of latest
consumer = KafkaConsumer('stocks-json')

# Consumer starting from earliest
# consumer = KafkaConsumer('stocks-json', auto_offset_reset='earliest')

# Consumer starting from specific offset
# consumer = KafkaConsumer()
# partition = TopicPartition('stocks-json', 0)
# start = 2700
# consumer.assign([partition])
# consumer.seek(partition, start)

print("consumer = ", consumer)
for event in consumer:
   print('Raw Message:', event)
   key = event.key
   value = event.value
   key2 = uuid.UUID(bytes = key)
   value2 = json.loads(value)
   print("Converted Message Value:", event.offset, key2, value2)
   # print(value2['symbol'], value2['quantity'])
   print('-' * 80)
