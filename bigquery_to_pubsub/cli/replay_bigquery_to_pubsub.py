# MIT License
#
# Copyright (c) 2019 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the 'Software'), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import datetime

import click

from bigquery_to_pubsub.exporters.console_item_exporter import ConsoleItemExporter
from bigquery_to_pubsub.exporters.google_pubsub_item_exporter import GooglePubSubItemExporter
from bigquery_to_pubsub.service.replay_job import ReplayJob
from bigquery_to_pubsub.service.replayer import Replayer
from bigquery_to_pubsub.service.time_series_bigquery_to_file_service import TimeSeriesBigQueryToFileService
from bigquery_to_pubsub.utils.logging_utils import logging_basic_config

logging_basic_config()

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-t', '--bigquery-table', default='bigquery-public-data.crypto_ethereum.blocks', type=str, help='')
@click.option('-s', '--start-timestamp', default='2019-10-23T00:00:00',
              type=click.DateTime(formats=[DATETIME_FORMAT]), help='')
@click.option('-e', '--end-timestamp', default='2019-10-23T02:00:00',
              type=click.DateTime(formats=[DATETIME_FORMAT]), help='')
@click.option('-r', '--replay-start-timestamp', default=datetime.datetime.now().strftime(DATETIME_FORMAT),
              type=click.DateTime(formats=[DATETIME_FORMAT]), help='')
@click.option('-b', '--replay-rate', default=1, type=float, help='Replay rate. Number between 0 and 1.')
@click.option('-b', '--batch-size-in-seconds', default=3600, type=int, help='')
@click.option('-f', '--timestamp-field', default='timestamp', type=str, help='')
@click.option('-p', '--pubsub-topic', default=None, type=str, help='')
@click.option('--temp-bigquery-dataset', required=True, type=str, help='')
@click.option('--temp-bucket', required=True, type=str, help='')
def replay_bigquery_to_pubsub(bigquery_table, start_timestamp, end_timestamp, replay_start_timestamp, replay_rate,
                       batch_size_in_seconds, timestamp_field, pubsub_topic, temp_bigquery_dataset, temp_bucket):

    # TODO: Create temp BigQuery dataset and storage bucket if don't exist
    time_series_bigquery_to_file_service = TimeSeriesBigQueryToFileService(
        bigquery_table=bigquery_table,
        timestamp_field=timestamp_field,
        temp_bigquery_dataset=temp_bigquery_dataset,
        temp_bucket=temp_bucket
    )

    item_exporter = create_item_exporter(pubsub_topic)

    replayer = Replayer(
        time_series_start_timestamp=start_timestamp,
        replay_start_timestamp=replay_start_timestamp,
        timestamp_field=timestamp_field,
        replay_rate=replay_rate,
        item_exporter=item_exporter
    )

    job = ReplayJob(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        batch_size_in_seconds=batch_size_in_seconds,
        replayer=replayer,
        time_series_bigquery_to_file_service=time_series_bigquery_to_file_service
    )

    job.run()


def create_item_exporter(pubsub_topic):
    if pubsub_topic is not None:
        item_exporter = GooglePubSubItemExporter(topic=pubsub_topic)
    else:
        item_exporter = ConsoleItemExporter()

    return item_exporter
