#! /usr/bin/python3

import json
import argparse
 
from kafka import KafkaConsumer, TopicPartition

def consume_json_data(bootstrap_servers = 'localhost:9092', topic = 'stocks-json2', partition = 0):
   consumer = KafkaConsumer()
   consumer.assign([TopicPartition(topic, partition)])
   #consumer.assign([TopicPartition(topic, 0), TopicPartition(topic, 1)])
   print("consumer = ", consumer)
   for event in consumer:
      key = str(event.key)
      value = json.loads(event.value)
      print("\npartition", event.partition, "\noffset", event.offset, "\nkey", key, "\nmessage", value)


def main():
   parser = argparse.ArgumentParser()
   parser.add_argument(
      '-b', '--bootstrap_servers', required=False, type=str, default='localhost:9092')
   parser.add_argument(
      '-t', '--topic', required=False, type=str, default='stocks-json2')
   parser.add_argument(
      '-p', '--partition', required=False, type=int, default=0)

   args = parser.parse_args()
   print("Args", args)

   consume_json_data(bootstrap_servers = args.bootstrap_servers, topic = args.topic, partition = args.partition)

if __name__ == '__main__':
   main()

