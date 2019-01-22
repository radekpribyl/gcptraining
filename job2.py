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

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

INPUT_TOPIC='projects/monster-datalake-dev-297a/topics/tigers-tst'
OUTPUT_BUCKET='gs://bck-msr-datalake-dev-importtest/radek-tst/job.json'
OUTPUT_TABLE='monster-datalake-dev-297a:tiger_dataset.radek_tst'

logger = logging.getLogger('job2_beam')

class ConverToBqRecordDoFn(beam.DoFn):
    
    schema = {"postingid": (1,0), "jobtitle": (1,0), "jobbody": (0,0),  "company": (0,1), "fields_company": {"name": (0,0), "normalizedcompanyid":(0,0), "normalizedcompanyname": (0,0)}, "generateddate": (0,0), "jobadpricingtypeid": (0,0)}

    def process(self, element):
        element_dict = json.loads(element)
        table_row = self._create_dict_from(self.schema, element_dict)
        if table_row:
            yield table_row
        else:
            return

    def _create_dict_from(self, schema_dict, element_dict):
        result = {}
        for key, value in schema_dict.iteritems():
            if key.startswith('fields_'):
                continue
            required, nested = value
            if key in element_dict:
                if nested:
                    result[key]=self._create_dict_from(self.schema["fields_"+key], element_dict[key])
                else:
                    result[key]=element_dict[key]
            elif required:
                print 'Missing required field'
                logger.info('Missing required field')
                return
            else:
                result[key]=None
        return result

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
    return (pipeline | "read from pubsub" >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC))#.with_output_types(bytes))

def convert_to_json(jobs_in_bytes):
    return jobs_in_bytes | 'Convert to json' >> beam.Map(lambda x: [json.loads(x)])

def create_bqrow_record(jobs_in_json):
    return jobs_in_json | 'Create Bq Record' >> beam.ParDo(ConverToBqRecordDoFn())

def save_to_gs(jobs_in_json):
    jobs_in_json | beam.io.WriteToText(OUTPUT_BUCKET)

def save_to_bigquery(job_rows):
    job_rows | 'Write to Bigquery' >> beam.io.WriteToBigQuery(
        OUTPUT_TABLE,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

def start(argv=None):
    pipeline = create_pipeline(argv)
    jobs_in_bytes = read_from_pubsub(pipeline)
    #jobs_in_json = convert_to_json(jobs_in_bytes)
    #save_to_gs(jobs_in_bytes)
    bqrows = create_bqrow_record(jobs_in_bytes)
    save_to_bigquery(bqrows)
    result = pipeline.run()
    result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  start()
