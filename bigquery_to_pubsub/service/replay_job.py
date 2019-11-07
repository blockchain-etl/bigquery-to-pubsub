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

import decimal
import json
from datetime import timedelta

from bigquery_to_pubsub.utils.file_utils import delete_file


class ReplayJob:
    def __init__(
            self,
            start_timestamp,
            end_timestamp,
            batch_size_in_seconds,
            time_series_bigquery_to_file_service,
            replayer):
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.batch_size_in_seconds = batch_size_in_seconds

        self.time_series_bigquery_to_file_service = time_series_bigquery_to_file_service
        self.replayer = replayer

    def run(self):
        try:
            self._do_run()
        finally:
            self._end()

    def _do_run(self):
        batches = split_to_batches(self.start_timestamp, self.end_timestamp, self.batch_size_in_seconds)

        for batch_start_timestamp, batch_end_timestamp in batches:
            file = self.time_series_bigquery_to_file_service.download_time_series(batch_start_timestamp, batch_end_timestamp)

            with open(file) as file_handle:
                for line in file_handle:
                    item = json.loads(line, parse_float=decimal.Decimal)
                    self.replayer.replay(item)
            delete_file(file)

    def _end(self):
        self.time_series_bigquery_to_file_service.close()


def split_to_batches(start_timestamp, end_timestamp, batch_size_in_seconds):
    batch_start = start_timestamp
    while batch_start < end_timestamp:
        batch_end = min(end_timestamp, batch_start + timedelta(seconds=batch_size_in_seconds))
        yield batch_start, batch_end
        batch_start = batch_end
