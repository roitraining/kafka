#! /usr/bin/python3
from pykafka import KafkaClient
import json
import random
import threading
import time
import os

producer_sleep_time = 4
stocks = ['AAPL', 'GOOG', 'MSFT']
kafka_topic='stocks'

def produce_data():
    hosts = 'localhost:9092'
    client = KafkaClient(hosts=hosts)
    print('client topics:', client.topics)

    topic = client.topics[bytes(kafka_topic, encoding = 'utf-8')]
    producer = topic.get_producer()

    def stock_message(stock_number):
        while True:
            msg = json.dumps({
                'timestamp': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())),
                'symbol': stocks[stock_number],
                'price': random.randint(100, 300),
            })
            #os.system('clear')
            print('msg:', msg)
            producer.produce(bytes(msg, encoding='utf-8'))
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
    produce_data()
