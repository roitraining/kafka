#! /usr/bin/python3

# We need to make sure kafka-python installed
# pip install kafka-python

# make sure there is a topic called stocks-json
# kafka-topics.sh --bootstrap-server localhost:9092 --create --topic stocks-json
# kafka-topics.sh --bootstrap-server localhost:9092 --list

from kafka import KafkaProducer
import json
import random
import threading
import time
import os
import argparse

import io
import uuid

def produce_json_data(bootstrap_servers = 'localhost:9092', topic = 'stocks-json2', option = '1'):
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
    producer_sleep_time = 4
    stocks = ['AAPL', 'GOOG', 'MSFT']

    def stock_message(stock_number):
        while True:
            msg = json.dumps({
                'event_time': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())),
                'symbol': stocks[stock_number],
                'price': random.randint(10000, 30000)/100,
                'quantity': random.randint(10, 1000)
            })

            if option == '1':
                key = None
                producer.send(topic, value=str.encode(msg))
            elif option == '2':
                key = uuid.uuid4()
                producer.send(topic, key=key.bytes, value=str.encode(msg))
            elif option == '3':
                key = stocks[stock_number]
                producer.send(topic, key=str.encode(key), value=str.encode(msg))
            elif option == '4':
                key = stocks[stock_number]
                p = 0 if stock_number == 0 else 1
                producer.send(topic, key=str.encode(key), value=str.encode(msg), partition=p)
 
            print('json producer -', 'key:', key, 'msg:', msg)
            time.sleep(producer_sleep_time)

    thread_list = [threading.Thread(target=stock_message, args=(i,)) for i in range(len(stocks))]
    for thread in thread_list:
        thread.setDaemon(True)
        thread.start()

    # block it to run forever
    while True:
        time.sleep(600)
        pass

def main():
   parser = argparse.ArgumentParser()
   parser.add_argument(
      '-b', '--bootstrap_servers', required=False, type=str, default='localhost:9092')
   parser.add_argument(
      '-t', '--topic', required=False, type=str, default='stocks-json2')
   parser.add_argument(
      '-o', '--option', required=False, type=str, default='1')

   args = parser.parse_args()
   print(args)


   produce_json_data(bootstrap_servers = args.bootstrap_servers, topic = args.topic, option = args.option)

if __name__ == '__main__':
   main()






"""

#! /usr/bin/python3
# /usr/local/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic json-stocks 
# /usr/local/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 

from kafka import KafkaProducer
import json
import random
import threading
import time
import os

producer_sleep_time = 4
stocks = ['AAPL', 'GOOG', 'MSFT']

kafka_topic='json-stocks'

key = 1
def produce_json_data():
    hosts = 'localhost:9092'
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def stock_message(stock_number):
        global key
        while True:
            msg = json.dumps({
                'event_time': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())),
                'symbol': stocks[stock_number],
                'price': random.randint(100, 300),
                'quantity': random.randint(10, 1000)
            })
            print('key:', key, 'msg:', msg)
            producer.send(kafka_topic, key=str.encode(str(key)), value=str.encode(msg))
            key += 1
            time.sleep(producer_sleep_time)

    thread_list = [threading.Thread(target=stock_message, args=(i,)) for i in range(len(stocks))]
    for thread in thread_list:
        thread.setDaemon(True)
        thread.start()

    # block it to run forever
    while True:
        time.sleep(600)
        pass

if __name__ == '__main__':
    produce_json_data()
"""