#! /usr/bin/python3

from kafka import KafkaConsumer, TopicPartition
c = KafkaConsumer('stocks-json', max_poll_records = 1)
x = c.poll()
len(x[TopicPartition(topic='stocks-json', partition=0)])
