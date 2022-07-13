#! /usr/bin/python3

from kafka import KafkaConsumer, TopicPartition
c = KafkaConsumer('stocks-json', bootstrap_servers = 'localhost:9092', max_poll_records = 10)
while True:
    x = c.poll()
#    print(len(x))
    if len(x) > 0:
        len(x[TopicPartition(topic='stocks-json', partition=0)])
