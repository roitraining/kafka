#! /usr/bin/python3
# pip install avro-python3
# pip install avro-json-serializer
# pip install mysql-connector-python

from kafka import KafkaConsumer
import io
import uuid
import argparse
import fastavro
import mysql.connector


cn = None
mycursor = None
stock_schema = fastavro.schema.load_schema("stock.avsc")

def avro_to_dict(msg, schema):
   print('msg.value ---> ', msg)

   buf = io.BytesIO(msg)
   buf.seek(0)
   ret = fastavro.schemaless_reader(buf, schema)

   return ret

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
            print(event)
            try:
               x = avro_to_dict(event.value, stock_schema)
               print('\ndeserialized -', event.offset, uuid.UUID(bytes=event.key), event.timestamp, '\n', x)
               print(event)
            except:
               print('bad')
            print('-' * 80)
         else:
            insert_sql(event)
      except:
         print('sql error')

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



# ConsumerRecord(topic='stocks-avro', partition=0, offset=7012, timestamp=1645157200910, timestamp_type=0, key=b'\\D\xdd\xe5\xab\xfbFM\xb5\x86\x14l\xd1\x9f6\x15', value=b'{2022-02-18 04:06:40, GOOG, 245.81, 527}', headers=[], checksum=None, serialized_key_size=16, serialized_value_size=40, serialized_header_size=-1)
# ConsumerRecord(topic='stocks-avro', partition=0, offset=7021, timestamp=1645157212934, timestamp_type=0, key=b'A\x10\xc4\x90\xeb\xaaIF\x9b\xe7\xee\xbb\x8f\xcc}\x91', value=b'{2022-02-18 04:06:52, GOOG, 113.42, 709}', headers=[], checksum=None, serialized_key_size=16, serialized_value_size=40, serialized_header_size=-1)


# avro consumer - de7c34e9-4729-4c8e-bb0b-597ad78f05bb 1645157339013 {'event_time': '2022-02-18 04:08:59', 'symbol': 'AAPL', 'price': 175.72000122070312, 'quantity': 170}
# ConsumerRecord(topic='stocks-avro', partition=0, offset=7069, timestamp=1645157339013, timestamp_type=0, key=b'\xde|4\xe9G)L\x8e\xbb\x0bYz\xd7\x8f\x05\xbb', value=b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xca\x03{"type": "record", "name": "Stock", "namespace": "example.avro", "fields": [{"type": "string", "name": "event_time"}, {"type": "string", "name": "symbol"}, {"type": "float", "name": "price"}, {"type": "int", "name": "quantity"}]}\x00\xad\x9d,%p\xbeX/m+\x9b\xe4\x98r\x1a\xde\x02>&2022-02-18 04:08:59\x08AAPLR\xb8/C\xd4\x02\xad\x9d,%p\xbeX/m+\x9b\xe4\x98r\x1a\xde', headers=[], checksum=None, serialized_key_size=16, serialized_value_size=330, serialized_header_size=-1)
# ConsumerRecord(topic='stocks-avro', partition=0, offset=7111, timestamp=1645157396681, timestamp_type=0, key=b'j\xa2_\xf1B\xe6OA\x97\xa7\x12\xa7\xbe\xe74s', value=b'{2022-02-18 04:09:56, MSFT, 296.69, 420}', headers=[], checksum=None, serialized_key_size=16, serialized_value_size=40, serialized_header_size=-1)
