#! /usr/local/python3
# /usr/local/flink/bin/flink run --python beam-kafka-consumer-print.py 
# python -m pip install apache-flink
import sys
print(sys.version)
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io.external.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import typing


brokers = 'localhost:9092'
kafka_topic = 'stocks-avro'
#kafka_topic = 'stocks2'

def convert_kafka_record_to_dictionary(record):
    # the records have 'value' attribute when --with_metadata is given
    if hasattr(record, 'value'):
      stock_bytes = record.value
    elif isinstance(record, tuple):
      stock_bytes = record[1]
    else:
      raise RuntimeError('unknown record type: %s' % type(record))
    # Converting bytes record from Kafka to a dictionary.
    import ast
    stock = ast.literal_eval(stock_bytes.decode("UTF-8"))
    output = {
        key: stock[key]
        for key in ['timestamp', 'symbol', 'price']
    }
    if hasattr(record, 'timestamp'):
      # timestamp is read from Kafka metadata
      output['timestamp'] = record.timestamp
    print (record, output)
    return output

def printit(x):
  print('x', x)

def log(stock):
  logging.info(stock)

options = PipelineOptions()

kafka_config = {
                  'bootstrap.servers': brokers
                }

print('start of pipeline')
with beam.Pipeline(options = options) as p:
    (p
      | 'Read from Kafka' >> ReadFromKafka(consumer_config=
                                {
                                 'bootstrap.servers': brokers
                                }
                            , topics=[kafka_topic]) #, with_metadata = True)
     #| 'Convert dict to byte string' >> beam.Map(lambda x: (b'', json.dumps(x).encode('utf-8')))
     | beam.Map(lambda x : (0,x)).with_output_types(typing.Tuple[bytes, bytes])
      | WriteToKafka(producer_config={'bootstrap.servers': brokers}, topic="classroom")
        #      | 'Print' >> beam.Map(lambda x : print('*' * 100, '\n', x))
    )
