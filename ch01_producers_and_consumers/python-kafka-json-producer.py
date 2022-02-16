#! /usr/bin/python3
# pip install kafka-python
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
