#! /usr/bin/python3

import json
import random
import datetime, time
import os
import argparse

import io
import uuid

stocks = ['AAPL', 'GOOG', 'MSFT']
base_time = 1645290182886
with open('trades.txt', 'w') as f:
    t = datetime.datetime.now()
    five_minutes = datetime.timedelta(minutes = 5)
    for x in range(1000):
        t2 = t + five_minutes * x
        for s in range(3):
            msg = {
#                'event_time': str(time.strftime("%Y-%m-%d %H:%M:%S", t2)),
                'event_time': str(t2.strftime("%Y-%m-%d %H:%M:%S")),
                'symbol': stocks[s],
                'price': random.randint(10000, 30000)/100,
                'quantity': random.randint(10, 1000)
            }
            key = uuid.uuid4()
            timestamp = time.mktime(t2.timetuple())

            data = {"key": str(key), "value": msg
            , "timestamp": timestamp, "offset": x
            , "partition": 0, "timestamp_type" : 0, "topic": "stocks-json"
            }
            f.write(json.dumps(data) + '\n')

