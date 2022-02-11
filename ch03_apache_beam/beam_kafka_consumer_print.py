import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io.external.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions


brokers = 'localhost:9092'
kafka_topic = 'stocks'


def run_pipeline():
  options = PipelineOptions(
#      runner = "DirectRunner",
      runner = "PortableRunner",
      job_endpoint = "localhost:8099",
      environment_type = "LOOPBACK"
  )
  #print(options)

# options = PipelineOptions([
#     "--runner=PortableRunner",
#     "--job_endpoint=localhost:8099",
#     "--environment_type=LOOPBACK"
# ])

  # beam_options = PipelineOptions(
  #   beam_args,
  #   runner='DataflowRunner',
  #   project='my-project-id',
  #   job_name='unique-job-name',
  #   temp_location='gs://my-bucket/temp',
  #   region='us-central1')


  #pipeline_options = PipelineOptions()
#  with beam.Pipeline(options = options) as p:
  with beam.Pipeline() as p:
    (p
#    | beam.Create(['alpha','beta', 'gamma'])
      | 'Read from Kafka' >> ReadFromKafka(consumer_config=
                                {
                                 'bootstrap.servers': brokers
                                ,'auto.offset.reset': 'latest'
                                ,'session.timeout.ms': '12000'
#                                ,'request.timeout.ms.config': 120000
                                }
                            , topics=[kafka_topic])
      | 'Print' >> beam.Map(lambda x : print('*' * 100, '\n', x))
    )
    #  | 'Window of 10 seconds' >> beam.WindowInto(window.FixedWindows(10))
    #  | 'Group by key' >> beam.GroupByKey()
    #  | 'Sum word counts' >> beam.Map(lambda kv: (kv[0], sum(kv[1])))
    #  | 'Write to Kafka' >> WriteToKafka(producer_config={'bootstrap.servers': kafka_bootstrap},
    #                                     topic='demo-output'))
    # )

if __name__ == '__main__':
   run_pipeline()
    