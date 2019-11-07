# MIT License
#
# Copyright (c) 2019 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import logging

from google.cloud import bigquery

from bigquery_to_pubsub.utils.gcs_utils import download_from_gcs, delete_in_gcs
from bigquery_to_pubsub.utils.random_utils import random_string


class BigQueryToFileJob:

    def __init__(
            self,
            sql,
            output_filename,
            temp_bigquery_dataset,
            temp_bucket):
        self.sql = sql
        self.output_filename = output_filename

        self.temp_bigquery_dataset = temp_bigquery_dataset
        self.temp_bucket = temp_bucket

        self.bigquery_client = bigquery.Client()

    def run(self):
        logging.info('Started BigQueryToFileJob with SQL {} and output filename {}'.format(
            self.sql, self.output_filename
        ))

        # Query
        # TODO: Make sure it works with different locations.

        random_name = random_string(10)
        destination_table = self.bigquery_client.dataset(self.temp_bigquery_dataset).table(
            random_name)
        query_job_config = bigquery.QueryJobConfig()
        query_job_config.priority = bigquery.QueryPriority.INTERACTIVE
        query_job_config.destination = destination_table

        query_job = self.bigquery_client.query(
            self.sql,
            job_config=query_job_config
        )

        submit_bigquery_job(query_job, query_job_config)
        assert query_job.state == 'DONE'

        # Export
        # TODO: Allow exporting to multiple files in case output is bigger than 1GB.

        bucket = self.temp_bucket
        filename = random_name + '.json'
        object = filename
        destination_uri = "gs://{}/{}".format(bucket, object)
        extract_job_config = bigquery.ExtractJobConfig()
        extract_job_config.priority = bigquery.QueryPriority.INTERACTIVE
        extract_job_config.destination_format = bigquery.job.DestinationFormat.NEWLINE_DELIMITED_JSON

        extract_job = self.bigquery_client.extract_table(
            destination_table,
            destination_uri,
            job_config=extract_job_config
        )
        submit_bigquery_job(extract_job, extract_job_config)
        assert query_job.state == 'DONE'

        # Delete the BigQuery table

        self.bigquery_client.delete_table(destination_table)

        # Download

        download_from_gcs(bucket, object, self.output_filename)
        delete_in_gcs(bucket, object)


def submit_bigquery_job(job, configuration):
    try:
        logging.info('Creating a job: ' + json.dumps(configuration.to_api_repr()))
        result = job.result()
        assert job.errors is None or len(job.errors) == 0
        return result
    except Exception:
        logging.info(job.errors)
        raise
