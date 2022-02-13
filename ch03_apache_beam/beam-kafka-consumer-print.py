# /usr/local/flink/bin/flink run --python beam-kafka-consumer-print.py 
import sys
print(sys.version)
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io.external.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import typing


brokers = 'localhost:9092'
kafka_topic = 'stocks'
kafka_topic2 = 'stocks2'

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
  print('x')

def log(stock):
  logging.info(stock)
  # if 'timestamp' in stock:
  #   #logging.info(f"stock {stock['timestamp']}, {stock['symbol']}, {stock['price']}")
  #   logging.info(stock)
  # else:
  #   logging.info('error')

options = PipelineOptions(
#      runner = "SparkRunner"
#      runner = "PortableRunner"
      runner = "FlinkRunner"
      , flink_master="localhost:8081"
      , environment_type="LOOPBACK"
      , streaming="true"
      #, checkpointing_interval=1000
      #, checkpointingInterval=30000, env="dev"
#      environment_type = "DOCKER"
  )

kafka_config = {
                  'bootstrap.servers': brokers
                }

with beam.Pipeline(options = options) as p:
    (p
      # | 'Read from Kafka' >> ReadFromKafka(consumer_config = kafka_config
      #       , topics=[kafka_topic] , with_metadata = True)
    | beam.Create(['a','b','c'])
    | beam.Map(log)
#       | 'Read from Kafka' >> ReadFromKafka(consumer_config=
#                                 {
#                                  'bootstrap.servers': brokers
# #                                ,'auto.offset.reset': 'latest'
# #                                ,'session.timeout.ms': '12000'
#                                 }
#                             , topics=[kafka_topic], with_metadata = True)
#        | beam.Map(print)
#       | beam.Map(lambda record: convert_kafka_record_to_dictionary(record))                            
      # | beam.FlatMap(lambda stock: log_stock(stock))
#       | beam.WindowInto(beam.window.FixedWindows(3))
#      | 'Print' >> beam.Map(lambda x : print('test', x))
#      | WriteToKafka(producer_config={'bootstrap.servers': brokers}, topic=kafka_topic2)
        #      | 'Print' >> beam.Map(lambda x : print('*' * 100, '\n', x))
    )

# def run_pipeline():
#   options = PipelineOptions(
# #      runner = "DirectRunner",
#       runner = "PortableRunner",
#       environment_type = "DOCKER"
# #      job_endpoint = "localhost:8099",
#       #      environment_type = "LOOPBACK"
#   )
#   #print(options)

#   # python path/to/my/pipeline.py \
#   # --runner=PortableRunner \
#   # --job_endpoint=ENDPOINT \
#   # --environment_type=DOCKER \



# # options = PipelineOptions([
# #     "--runner=PortableRunner",
# #     "--job_endpoint=localhost:8099",
# #     "--environment_type=LOOPBACK"
# # ])

#   # beam_options = PipelineOptions(
#   #   beam_args,
#   #   runner='DataflowRunner',
#   #   project='my-project-id',
#   #   job_name='unique-job-name',
#   #   temp_location='gs://my-bucket/temp',
#   #   region='us-central1')


#   #pipeline_options = PipelineOptions()
# #  with beam.Pipeline(options = options) as p:
#   with beam.Pipeline() as p:
#     (p
# #    | beam.Create(['alpha','beta', 'gamma'])
#       | 'Read from Kafka' >> ReadFromKafka(consumer_config=
#                                 {
#                                  'bootstrap.servers': brokers
#                                 ,'auto.offset.reset': 'latest'
#                                 ,'session.timeout.ms': '12000'
# #                                ,'request.timeout.ms.config': 120000
#                                 }
#                             , topics=[kafka_topic])
#       | 'Print' >> beam.Map(lambda x : print('*' * 100, '\n', x))
#     )
#     #  | 'Window of 10 seconds' >> beam.WindowInto(window.FixedWindows(10))
#     #  | 'Group by key' >> beam.GroupByKey()
#     #  | 'Sum word counts' >> beam.Map(lambda kv: (kv[0], sum(kv[1])))
#     #  | 'Write to Kafka' >> WriteToKafka(producer_config={'bootstrap.servers': kafka_bootstrap},
#     #                                     topic='demo-output'))
#     # )

# if __name__ == '__main__':
#    run_pipeline()
    

# with beam.Pipeline(options = options) as p:
#     (p
#       | 'Read from Kafka' >> beam.Create(['ab','cd','ef'])
#       | 'Print' >> beam.Map(lambda x : print('*' * 100, '\n', x))
#     )


# import apache_beam as beam
# from apache_beam.options.pipeline_options import PipelineOptions

# options = PipelineOptions(
#       runner = "SparkRunner",
#   )
# with beam.Pipeline() as p:
#     (p
#       | 'Read from Kafka' >> beam.Create(['ab','cd','ef'])
#       | 'Print' >> beam.Map(lambda x : print('*' * 100, '\n', x))
#     )
