#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A streaming word-counting workflow.
"""

from __future__ import absolute_import

import argparse
import logging
import json

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

INPUT_TOPIC='projects/monster-datalake-dev-297a/topics/tigers-tst'
OUTPUT_BUCKET='gs://bck-msr-datalake-dev-importtest/radek-tst/job.json'

def create_pipeline(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    return beam.Pipeline(options=pipeline_options)

def read_from_pubsub(pipeline):
    return (pipeline | "read from pubsub" >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC).with_output_types(bytes))

def convert_to_json(jobs_in_bytes):
    return jobs_in_bytes | 'Convert to json' >> beam.Map(lambda x: json.loads(x))

def save_to_gs(jobs_in_json):
    jobs_in_json | beam.io.WriteToText(OUTPUT_BUCKET)

def start(argv=None):
    pipeline = create_pipeline(argv)
    jobs_in_bytes = read_from_pubsub(pipeline)
    jobs_in_json = convert_to_json(jobs_in_bytes)
    save_to_gs(jobs_in_json)
    result = pipeline.run()
    result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  start()
