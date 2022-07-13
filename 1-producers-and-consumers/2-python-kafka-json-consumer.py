#! /usr/bin/python3
# start 1-python-kafka-json-producer.py in one terminal window, then start this program in another terminal window 

import json, uuid
from kafka import KafkaConsumer, TopicPartition

# Consumer starting from default of latest
bootstrap_servers = 'localhost:9092'
consumer = KafkaConsumer('stocks-json', bootstrap_servers = bootstrap_servers)

# Consumer starting from earliest
# consumer = KafkaConsumer('stocks-json', auto_offset_reset='earliest')

# Consumer starting from specific offset
# consumer = KafkaConsumer()
# partition = TopicPartition('stocks-json', 0)
# #partition2 = TopicPartition('stocks-json', 1)
# consumer.assign([partition])
# start = 5354
# consumer.seek(partition, start)

print("consumer = ", consumer)
for event in consumer:
   print('Raw Message:', event, type(event))
   key = event.key
   value = event.value
   #print("Converted Message Value:", event.offset, key, value)
   #key2 = uuid.UUID(bytes = key)
   key2 = str(key)
   value2 = json.loads(value)
   print("Converted Message Value:", event.offset, key2, value2, value2['symbol'])
   # print(value2['symbol'], value2['quantity'])
   print('-' * 80)
