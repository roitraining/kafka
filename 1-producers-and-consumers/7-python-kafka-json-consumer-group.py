#! /usr/bin/python3

import json, uuid
import argparse
import time
 
from kafka import KafkaConsumer, TopicPartition

def consume_json_data(bootstrap_servers = 'localhost:9092', topic = 'stocks-json', partition = None, group_id = 'group1'):
   if partition is None:
      consumer = KafkaConsumer(topic, group_id = group_id, auto_offset_reset='latest')
   else:
      consumer = KafkaConsumer(group_id = group_id, auto_offset_reset='latest')
      consumer.assign([TopicPartition(topic, partition)])

   input(f"consumer =  {consumer} press any key to continue")
   for event in consumer:
      #key = str(event.key)
      value = json.loads(event.value)
      print("\npartition", event.partition, "\noffset", event.offset, "\nkey", event.key, "\nmessage", value)


def main():
   parser = argparse.ArgumentParser()
   parser.add_argument(
      '-b', '--bootstrap_servers', required=False, type=str, default='localhost:9092')
   parser.add_argument(
      '-t', '--topic', required=False, type=str, default='stocks-json')
   parser.add_argument(
      '-p', '--partition', required=False, type=int, default=None)
   parser.add_argument(
      '-g', '--group', required=False, type=str, default='group1')

   args = parser.parse_args()
   print("Args", args)

   consume_json_data(bootstrap_servers = args.bootstrap_servers, topic = args.topic, partition = args.partition, group_id = args.group)

if __name__ == '__main__':
   main()

