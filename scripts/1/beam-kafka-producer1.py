import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io.external.kafka import ReadFromKafka, WriteToKafka
import typing, json
from apache_beam.options.pipeline_options import PipelineOptions

brokers = 'localhost:9092'
kafka_topic = 'classroom'

# options = PipelineOptions(
#       runner = "FlinkRunner"
#       #, streaming="true"
#       , flink_master="localhost:8081"
#       , environment_type="LOOPBACK"
#   )

options = PipelineOptions()

def run_pipeline(options = options):
  with beam.Pipeline() as p:
    (p
     | beam.Create([{'a' : 'alpha'}, {'b' : 'beta'}])
     | 'Convert dict to byte string' >> beam.Map(lambda x: (b'', json.dumps(x).encode('utf-8')))
     | beam.Map(lambda x : x).with_output_types(typing.Tuple[bytes, bytes])
#     | beam.Map(print)
     | WriteToKafka(producer_config = {'bootstrap.servers': brokers}
                            , topic=kafka_topic)
    )

if __name__ == '__main__':
  run_pipeline()
    