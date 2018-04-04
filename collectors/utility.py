import time

def get_timestamp_str(time_in_seconds, timezone_offset):
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(time_in_seconds + timezone_offset))

def current_timestamp_str():
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(time.time()))

def current_timestamp_int():
    return time.time()
