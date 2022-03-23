#! /usr/bin/python3

import json
import argparse
import time
import uuid
from kafka.structs import TopicPartition

from kafka import KafkaConsumer, TopicPartition

# enable_auto_commit
# auto_commit_interval_ms
# max_poll_records 


def consume_json_data(bootstrap_servers = 'localhost:9092', topic = 'stocks-json', group_id = 'group1'):
   consumer = KafkaConsumer(topic, group_id = group_id
   , enable_auto_commit = False
   , key_deserializer = lambda x : uuid.UUID(bytes = x)
   , value_deserializer = lambda x : json.loads(x))
   last_commit = consumer.committed(partition = TopicPartition(topic, 0))
   print("consumer = ", consumer, "last commit", last_commit)
   for event in consumer:
      key = event.key
      value = event.value
      print(event.offset, key, value)
      if event.offset % 100 != 0 or last_commit == event.offset:
         consumer.commit()
      else:
         quit()


def main():
   parser = argparse.ArgumentParser()
   parser.add_argument(
      '-b', '--bootstrap_servers', required=False, type=str, default='localhost:9092')
   parser.add_argument(
      '-t', '--topic', required=False, type=str, default='stocks-json')
   parser.add_argument(
      '-g', '--group', required=False, type=str, default='group1')

   args = parser.parse_args()
   print("Args", args)

   consume_json_data(bootstrap_servers = args.bootstrap_servers, topic = args.topic, group_id = args.group)

if __name__ == '__main__':
   main()

