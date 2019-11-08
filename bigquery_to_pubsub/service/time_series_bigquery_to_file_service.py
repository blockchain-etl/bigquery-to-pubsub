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

import logging
from datetime import timedelta

from bigquery_to_pubsub.executors.bounded_executor import BoundedExecutor
from bigquery_to_pubsub.executors.fail_safe_executor import FailSafeExecutor
from bigquery_to_pubsub.service.time_series_bigquery_to_file_job import TimeSeriesBigQueryToFileJob
from bigquery_to_pubsub.utils.file_utils import delete_file


class TimeSeriesBigQueryToFileService:

    def __init__(self,
                 bigquery_table,
                 timestamp_field,
                 temp_bigquery_dataset,
                 temp_bucket):
        self.bigquery_table = bigquery_table
        self.timestamp_field = timestamp_field

        self.temp_bigquery_dataset = temp_bigquery_dataset
        self.temp_bucket = temp_bucket

        self.executor = FailSafeExecutor(BoundedExecutor(bound=1, max_workers=1))

        self.lookahead_job_futures = {}

    def download_time_series(self, start_timestamp, end_timestamp):
        job_future = self.lookahead_job_futures.get((start_timestamp, end_timestamp))

        if job_future is not None:
            logging.info('Job for {} and {} has been scheduled before. Getting the result.'.format(start_timestamp, end_timestamp))
            output_filename = job_future.result()
            del self.lookahead_job_futures[start_timestamp, end_timestamp]
        else:
            self.clean_lookahead_jobs()
            output_filename = self._do_download_time_series(start_timestamp, end_timestamp)

        self.schedule_lookahead_jobs(start_timestamp, end_timestamp)

        return output_filename

    def schedule_lookahead_jobs(self, start_timestamp, end_timestamp):
        # TODO: Allow scheduling more than 1 lookahead job.
        # Schedule only 1 lookahead job for now
        seconds = (end_timestamp - start_timestamp).total_seconds()

        lookahead_start_timestamp = end_timestamp
        lookahead_end_timestamp = lookahead_start_timestamp + timedelta(seconds=seconds)

        logging.info('Scheduling lookahead job for {} and {}'.format(lookahead_start_timestamp, lookahead_end_timestamp))
        job_future = self.executor.submit(self._do_download_time_series, lookahead_start_timestamp, lookahead_end_timestamp)

        self.lookahead_job_futures[lookahead_start_timestamp, lookahead_end_timestamp] = job_future

    def _do_download_time_series(self, start_timestamp, end_timestamp):
        job = TimeSeriesBigQueryToFileJob(
            bigquery_table=self.bigquery_table,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            timestamp_field=self.timestamp_field,
            temp_bigquery_dataset=self.temp_bigquery_dataset,
            temp_bucket=self.temp_bucket
        )
        return job.run()

    def clean_lookahead_jobs(self):
        for future in self.lookahead_job_futures.values():
            output_file = future.result()
            delete_file(output_file)
        self.lookahead_job_futures = {}

    def close(self):
        self.executor.shutdown()
        self.clean_lookahead_jobs()
