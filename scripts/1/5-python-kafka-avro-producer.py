#! /usr/bin/python3
# pip install kafka-python
# pip install avro
# pip install avro-python3
# pip install mysql

# kafka-topics.sh --bootstrap-server localhost:9092 --create --topic stocks-avro
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
import avro.io
import avro.schema
import avro.datafile

stock_schema = avro.schema.parse(open("stock.avsc", "rb").read())

def json_to_avro(msg, schema):
    '''
    This will take a JSON message and convert it to avro format
    '''
    buf = io.BytesIO()
    writer = avro.datafile.DataFileWriter(buf, avro.io.DatumWriter(), schema)
    writer.append(msg)
    writer.flush()
    buf.seek(0)
    data = buf.read()
    return data

def produce_avro_data(bootstrap_servers = 'localhost:9092', topic = 'stocks-avro'):
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
    producer_sleep_time = 4
    stocks = ['AAPL', 'GOOG', 'MSFT']

    def stock_message(stock_number):
        while True:
            msg = {
                'event_time': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())),
                'symbol': stocks[stock_number],
                'price': random.randint(10000, 30000)/100,
                'quantity': random.randint(10, 1000)
            }
            key = uuid.uuid4()
            print('avro producer -', 'key:', key, 'msg:', msg)
            avro_msg = json_to_avro(msg, stock_schema)
            producer.send(topic, key=key.bytes, value=avro_msg)
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
      '-t', '--topic', required=False, type=str, default='stocks-avro')

   args = parser.parse_args()
   print(args)


   produce_avro_data(bootstrap_servers = args.bootstrap_servers, topic = args.topic)

if __name__ == '__main__':
   main()


