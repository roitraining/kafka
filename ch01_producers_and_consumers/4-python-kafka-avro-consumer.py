#! /usr/bin/python3
# pip install avro-python3
# pip install avro-json-serializer
# pip install mysql-connector-python

import io
import uuid
import argparse
import avro.io
import avro.schema
import avro.datafile
from avro_json_serializer import AvroJsonSerializer
from kafka import KafkaConsumer
import mysql.connector

cn = None
mycursor = None

def avro_to_dict(msg):
   buf = io.BytesIO()
   buf.seek(0)
   buf.write(msg)
   x1 = avro.datafile.DataFileReader(buf, avro.io.DatumReader())
   x2 = next(x1)
   return x2

def insert_sql(event):
   kafka_key = uuid.UUID(bytes=event.key)
   kafka_timestamp = event.timestamp
   d = avro_to_dict(event.value)
   event_time = d['event_time']
   symbol = d['symbol']
   price = d['price']
   quantity = d['quantity']
   sql = f"""INSERT INTO trades(kafka_key, kafka_timestamp, event_time, symbol, price, quantity)
   VALUES (UNHEX(REPLACE('{kafka_key}', '-', '')), {kafka_timestamp}, '{event_time}', '{symbol}', {price}, {quantity})
   """
   print(sql)
   mycursor.execute(sql)
   cn.commit()

def consume(**kvargs):
   print(kvargs)
   servers = kvargs['bootstrap_servers']
   if ',' in servers:
      kvargs['bootstrap_servers'] = servers.split(',')
   else:
      kvargs['bootstrap_servers'] = [servers]
   
   topics = kvargs['topics']
   if ',' in topics:
      topics = topics.split(',')
   else:
      topics = [topics]

   del kvargs['topics']
   
   print(topics, kvargs)
   consumer = KafkaConsumer(*topics, **kvargs)

   print("consumer = ", consumer)

   for event in consumer:
      try:
         if not cn:
            print(uuid.UUID(bytes=event.key), event.timestamp, avro_to_dict(event.value))
         else:
            insert_sql(event)
      except:
         pass

def main():
   global cn, mycursor
   parser = argparse.ArgumentParser()
   parser.add_argument(
      '-b', '--bootstrap_servers', required=False, type=str, default='localhost:9092')
   parser.add_argument(
      '-t', '--topics', required=False, type=str, default='stocks-avro')
   parser.add_argument(
      '-g', '--group_id', required=False, type=str, default='stock-group')
   parser.add_argument(
      '-i', '--auto_commit_interval_ms', required=False, type=int, default=1000)
   parser.add_argument(
      '-o', '--auto_offset_reset', required=False, type=str, default='latest')
   parser.add_argument(
      '-a', '--enable_auto_commit', required=False, default=True, type=lambda x: (str(x).lower() == 'true'))
   parser.add_argument(
      '-s', '--sql', required=False, default=False, type=lambda x: (str(x).lower() == 'true'))

   args = parser.parse_args()
   print(args)

   args = args.__dict__
   if args['sql']:
      cn = mysql.connector.connect(user='python', password='python', host='127.0.0.1', database='stocks')
      mycursor = cn.cursor()

   del args['sql']

   print(args)
   consume(**args)

if __name__ == '__main__':
   main()
