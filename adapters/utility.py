import time

def get_timestamp_str(time_in_seconds, timezone_offset):
    if isinstance(time_in_seconds, str) or isinstance(time_in_seconds, unicode):
        return time_in_seconds
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(time_in_seconds + timezone_offset))

def current_timestamp_str():
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(time.time()))

def current_timestamp_int():
    return time.time()
