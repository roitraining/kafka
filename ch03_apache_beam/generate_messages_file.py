#! /usr/bin/python3

import json
import random
import time
import os
import argparse

import io
import uuid

stocks = ['AAPL', 'GOOG', 'MSFT']
base_time = 1645290182886
with open('trades.txt', 'w') as f:
    for x in range(1000):
        for s in range(3):
            msg = json.dumps({
                'event_time': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())),
                'symbol': stocks[s],
                'price': random.randint(10000, 30000)/100,
                'quantity': random.randint(10, 1000)
            })
            key = uuid.uuid4()
            timestamp = base_time + x * 1000

            data = {"key": str(key), "value": msg
            , "timestamp": timestamp, "offset": x
            , "partition": 0, "timestamp_type" : 0, "topic": "stocks-json"
            }
            f.write(json.dumps(data))

