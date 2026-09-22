from datetime import timedelta


def make_out_of_order_timestamp(event_ts, seconds_behind=3):
    return event_ts - timedelta(seconds=seconds_behind)