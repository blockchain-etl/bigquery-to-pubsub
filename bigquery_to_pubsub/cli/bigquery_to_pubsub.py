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

import click

from bigquery_to_pubsub.service.time_series_bigquery_to_pubsub_job import TimeSeriesBigQueryToPubSubJob
from bigquery_to_pubsub.utils.logging_utils import logging_basic_config

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-t', '--bigquery-table', default='bigquery-public-data.crypto_ethereum.blocks', type=str, help='')
@click.option('-s', '--start-timestamp', default='2019-10-23T00:00:00',
              type=click.DateTime(formats=["%Y-%m-%dT%H:%M:%S"]), help='')
@click.option('-e', '--end-timestamp', default='2019-10-23T02:00:00',
              type=click.DateTime(formats=["%Y-%m-%dT%H:%M:%S"]), help='')
@click.option('-b', '--batch-size-in-seconds', default=3600, type=int, help='')
@click.option('-f', '--timestamp-field', default='timestamp', type=str, help='')
@click.option('-p', '--pubsub-topic', default='', type=str, help='')
@click.option('--temp-bigquery-dataset', required=True, type=str, help='')
@click.option('--temp-bucket', required=True, type=str, help='')
def bigquery_to_pubsub(bigquery_table, start_timestamp, end_timestamp, batch_size_in_seconds, timestamp_field,
                       pubsub_topic, temp_bigquery_dataset, temp_bucket):
    job = TimeSeriesBigQueryToPubSubJob(
        bigquery_table=bigquery_table,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        batch_size_in_seconds=batch_size_in_seconds,
        timestamp_field=timestamp_field,
        pubsub_topic=pubsub_topic,
        temp_bigquery_dataset=temp_bigquery_dataset,
        temp_bucket=temp_bucket
    )

    job.run()
