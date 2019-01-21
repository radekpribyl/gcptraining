import argparse
import json
import apache_beam as beam
import apache_beam.transforms.window as window

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

def create_pipeline():
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=pipeline_options)

def read_jobs_from_pubsub(p):
    return (p | "read from topic" >> beam.io.ReadFromPubSub(
        topic="projects/monster-datalake-dev-297a/topics/tigers-tst")

def convert_to_json(jobs_in_bytes):
    return jobs_in_bytes | 'Convert to json' >> beam.Map(lambda x: json.loads(x))

def save_to_gs(jobs_in_json):
    jobs_in_json | beam.io.WriteToText('gs://bck-msr-datalake-dev-importtest/radek-tst/job.json')

def run():
    p = create_pipeline()
    jobs_in_bytes = read_jobs_from_pubsub(p)
    jobs_in_json = convert_to_json(jobs_in_bytes)
    save_to_gs(jobs_in_json)
    p.run()

if __name__ == '__main__':
  run()

