import sys
print(sys.version)
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions(
      runner = "SparkRunner",
#      environment_type = "DOCKER"
  )
with beam.Pipeline() as p:
    (p
      | 'Read from Kafka' >> beam.Create(['ab','cd','ef'])
      | 'Print' >> beam.Map(lambda x : print('*' * 100, '\n', x))
    )

