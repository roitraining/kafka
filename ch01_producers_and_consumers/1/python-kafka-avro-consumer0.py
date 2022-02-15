# pip install avro-json-serializer
import io
import uuid
import avro.io
import avro.schema
import avro.datafile
from avro_json_serializer import AvroJsonSerializer
from kafka import KafkaConsumer
consumer = KafkaConsumer('stocks'
            , auto_offset_reset='latest'
            , enable_auto_commit=True
            , auto_commit_interval_ms=1000
            , group_id='stock-group')

print("consumer = ", consumer)

def avro_to_dict(msg):
   buf = io.BytesIO()
   buf.seek(0)
   buf.write(msg)
   x1 = avro.datafile.DataFileReader(buf, avro.io.DatumReader())
   x2 = next(x1)
   return x2

for event in consumer:
   try:
      print(uuid.UUID(bytes=event.key), event.timestamp, avro_to_dict(event.value))
   except:
      pass

