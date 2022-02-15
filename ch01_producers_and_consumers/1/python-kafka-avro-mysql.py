# pip install avro-json-serializer
# pip install mysql-connector
import io
import uuid
import avro.io
import avro.schema
import avro.datafile
from avro_json_serializer import AvroJsonSerializer
from kafka import KafkaConsumer
import mysql.connector

consumer = KafkaConsumer('stocks')
print("consumer = ", consumer)

def avro_to_dict(msg):
   buf = io.BytesIO()
   buf.seek(0)
   buf.write(msg)
   x1 = avro.datafile.DataFileReader(buf, avro.io.DatumReader())
   x2 = next(x1)
   return x2

cn = mysql.connector.connect(user='python', password='python', host='127.0.0.1', database='stocks')
mycursor = cn.cursor()

for event in consumer:
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


