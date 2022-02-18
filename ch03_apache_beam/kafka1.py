import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.external.kafka import ReadFromKafka, WriteToKafka
import typing, json

kafka_bootstrap = 'localhost:9092'

def run_pipeline():
  with beam.Pipeline(options=PipelineOptions()) as p:
    (p
     | 'Read from Kafka' >> ReadFromKafka(consumer_config={'bootstrap.servers': kafka_bootstrap,
                                                           'auto.offset.reset': 'latest'},
                                          topics=['words'])
#     | beam.Create('now is the time for all good men to come to the aid of their country'.split(' '))
    #  | 'Convert dict to byte string' >> beam.Map(lambda x: (b'', json.dumps(x).encode('utf-8')))
    #  | beam.Map(lambda x : x).with_output_types(typing.Tuple[bytes, bytes])
#     | beam.Create('now is the time for all good men to come to the aid of their country'.split(' '))
     | beam.Map(lambda x : (x.key, x.value))
#     | beam.Map(lambda x : (x[0].decode(), x[1].decode()))
    #  | 'Par with 1' >> beam.Map(lambda word: (word, 1))
    #  | 'Window of 10 seconds' >> beam.WindowInto(window.FixedWindows(10))
    #  | 'Group by key' >> beam.GroupByKey()
    #  | 'Sum word counts' >> beam.Map(lambda kv: (kv[0], sum(kv[1])))
    #  | beam.Map(lambda x : (str.encode(x[0]), str.encode(str(x[1])))).with_output_types(typing.Tuple[bytes, bytes])
     | 'Write to Kafka' >> WriteToKafka(producer_config={'bootstrap.servers': kafka_bootstrap},
                                        topic='classroom')
    )

if __name__ == '__main__':
  run_pipeline()


#python kafka1.py --runner=FlinkRunner --environment_type=DOCKER