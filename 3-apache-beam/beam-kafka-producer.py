import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io.external.kafka import ReadFromKafka, WriteToKafka
import typing, json
from apache_beam.options.pipeline_options import PipelineOptions

brokers = 'localhost:9092'
kafka_topic = 'stocks'

options = PipelineOptions(
      # runner = "FlinkRunner"
      # #, streaming="true"
      # , flink_master="localhost:8081"
      # , environment_type="LOOPBACK"
  )

kafka_config = {
                  'bootstrap.servers': brokers
                }

def run_pipeline(options = options):
  with beam.Pipeline() as p:
    (p
      | 'Read from Kafka' >> ReadFromKafka(consumer_config = kafka_config
            , topics=[kafka_topic]) #, with_metadata = True)
#      | beam.Map(lambda x : print(type(x)))
#     | 'Convert dict to byte string' >> beam.Map(lambda x: (b'', json.dumps(x).encode('utf-8')))
#     | beam.Map(lambda x : x).with_output_types(typing.Tuple[bytes, bytes])
#     | beam.Map(print)
     | WriteToKafka(producer_config = {'bootstrap.servers': brokers}
                            , topic='classroom')
    )

if __name__ == '__main__':
  run_pipeline()
    