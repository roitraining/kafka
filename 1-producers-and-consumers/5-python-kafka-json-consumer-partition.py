#! /usr/bin/python3

import json, uuid
import argparse
 
from kafka import KafkaConsumer, TopicPartition

def consume_json_data(bootstrap_servers = 'localhost:9092', topic = 'stocks-json2', partition = 0, option = 0):
   # consumer = KafkaConsumer('stocks-json2', bootstrap_servers = bootstrap_servers)

   consumer = KafkaConsumer(bootstrap_servers = bootstrap_servers)
   partition = 0
   consumer.assign([TopicPartition(topic, partition)])
   #consumer.assign([TopicPartition(topic, 0), TopicPartition(topic, 1)])
   #consumer.assign([TopicPartition('stocks-json', 0), TopicPartition('stocks-json2', 1)])
   print("consumer = ", consumer)
   for event in consumer:
      key = event.key
      if option == '1':
          key = str(key)
      elif option == '2':
          key = uuid.UUID(bytes = key)

      # key = str(event.key)
      # if key == 'AAPL':
      #    pass
      # else:
      #    pass
      
      value = json.loads(event.value)
      print("\ntopic", event.topic, "\npartition", event.partition, "\noffset", event.offset, "\nkey", key, "\nmessage", value)


def main():
   parser = argparse.ArgumentParser()
   parser.add_argument(
      '-b', '--bootstrap_servers', required=False, type=str, default='localhost:9092')
   parser.add_argument(
      '-t', '--topic', required=False, type=str, default='stocks-json2')
   parser.add_argument(
      '-p', '--partition', required=False, type=int, default=0)
   parser.add_argument(
      '-o', '--option', required=False, type=str, default='1')

   args = parser.parse_args()
   print("Args", args)

   consume_json_data(bootstrap_servers = args.bootstrap_servers, topic = args.topic, partition = args.partition, option = args.option)

if __name__ == '__main__':
   main()

