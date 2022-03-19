#! /usr/bin/python3

import json
import argparse
import time
 
from kafka import KafkaConsumer, TopicPartition

def consume_json_data(bootstrap_servers = 'localhost:9092', topic = 'stocks-json', group_id = 'group1'):
   consumer = KafkaConsumer(topic, group_id = group_id)
   print("consumer = ", consumer)
   for event in consumer:
      #print('json consumer -', event)
      key = event.key
      value = event.value
      value2 = json.loads(value)
      #print(type(value), type(value2), value2)
      print(value2['symbol'], value2['quantity'])
      #time.sleep(4)


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

