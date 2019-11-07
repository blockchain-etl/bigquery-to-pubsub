import datetime
import json
import logging
import time


class Replayer:
    def __init__(
            self,
            time_series_start_timestamp,
            replay_start_timestamp,
            timestamp_field,
            rate,
            item_exporter):

        if replay_start_timestamp < time_series_start_timestamp:
            raise ValueError('replay_start_timestamp must be equal or after time_series_start_timestamp')

        if rate < 0:
            raise ValueError('rate must be greater than 0')

        self.time_series_start_timestamp = time_series_start_timestamp
        self.replay_start_timestamp = replay_start_timestamp
        self.timestamp_field = timestamp_field
        self.rate = rate
        self.item_exporter = item_exporter

        self.replay_time_delta = datetime.timedelta(
            seconds=(replay_start_timestamp - time_series_start_timestamp).total_seconds())

    def replay(self, item):
        item_timestamp = item.get(self.timestamp_field)
        if not item_timestamp:
            raise ValueError('item doesn\'t have a timestamp field ' + json.dumps(item))

        adjusted_item_timestamp = self.adjust_item_timestamp(parse_timestamp(item_timestamp))

        now = datetime.datetime.now()
        item_wait_time = (adjusted_item_timestamp - now).total_seconds()

        if item_wait_time > 0:
            logging.info('Waiting {} seconds before replaying item with timestamp {}'
                         .format(item_wait_time, item_timestamp))
            time.sleep(item_wait_time)
        self.item_exporter.export_item(item)

    def adjust_item_timestamp(self, item_timestamp):
        # Adjust rate
        time_diff = (item_timestamp - self.time_series_start_timestamp).total_seconds()
        adjusted_time_diff = time_diff * self.rate
        rate_adjusted_timestamp = self.time_series_start_timestamp + datetime.timedelta(seconds=adjusted_time_diff)

        # Adjust for replay delta
        delta_adjusted_timestamp = rate_adjusted_timestamp + self.replay_time_delta

        return delta_adjusted_timestamp


def parse_timestamp(ts):
    try:
        return datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S UTC')
    except ValueError:
        return datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f UTC')