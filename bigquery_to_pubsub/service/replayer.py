import datetime
import json
import logging
import time


# TODO: Check it works with timezones correctly.
class Replayer:
    def __init__(
            self,
            time_series_start_timestamp,
            replay_start_timestamp,
            timestamp_field,
            replay_rate,
            item_exporter):

        if replay_start_timestamp < time_series_start_timestamp:
            raise ValueError('replay_start_timestamp must be equal or after time_series_start_timestamp')

        if replay_rate < 0:
            raise ValueError('rate must be greater than 0')

        self.time_series_start_timestamp = time_series_start_timestamp
        self.replay_start_timestamp = replay_start_timestamp
        self.timestamp_field = timestamp_field
        self.replay_rate = replay_rate
        self.item_exporter = item_exporter

        self.replay_delta = datetime.timedelta(
            seconds=(replay_start_timestamp - time_series_start_timestamp).total_seconds())

    def replay(self, item):
        item_timestamp = item.get(self.timestamp_field)
        if not item_timestamp:
            raise ValueError('item doesn\'t have a timestamp field ' + json.dumps(item))

        item_timestamp = parse_timestamp(item_timestamp)

        replay_timestamp = self.adjust_item_timestamp(item_timestamp)

        now = datetime.datetime.now()
        item_wait_time = (replay_timestamp - now).total_seconds()

        if item_wait_time > 0:
            logging.info('Waiting {} seconds before replaying item with timestamp {}'.format(item_wait_time, item_timestamp))
            time.sleep(item_wait_time)
        enriched_item = self.enrich_item(item, item_timestamp, replay_timestamp)
        self.item_exporter.export_item(enriched_item)

    def adjust_item_timestamp(self, item_timestamp):
        # Adjust for rate
        offset = (item_timestamp - self.time_series_start_timestamp).total_seconds()
        adjusted_offset = offset * self.replay_rate
        timestamp_adjusted_for_rate = self.time_series_start_timestamp + datetime.timedelta(seconds=adjusted_offset)

        # Adjust for replay_delta
        timestamp_adjusted_for_replay_delta = timestamp_adjusted_for_rate + self.replay_delta

        return timestamp_adjusted_for_replay_delta

    def enrich_item(self, item, item_timestamp, replay_timestamp):
        item['_offset'] = (item_timestamp - self.time_series_start_timestamp).total_seconds()
        item['_replay_timestamp'] = format_timestamp(replay_timestamp)
        item['_publish_timestamp'] = format_timestamp(datetime.datetime.now())
        return item


def parse_timestamp(ts):
    try:
        return datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S UTC')
    except ValueError:
        return datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f UTC')


def format_timestamp(ts):
    return datetime.datetime.strftime(ts, '%Y-%m-%d %H:%M:%S.%f UTC')