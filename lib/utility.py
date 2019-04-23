import time

def get_timestamp_str_date(time_in_seconds, timezone_offset):
    if isinstance(time_in_seconds, str) or isinstance(time_in_seconds, unicode):
        return time_in_seconds
    ret = time.strftime('%m-%d', time.localtime(time_in_seconds + timezone_offset))
    return ret

def get_timestamp_str_short(time_in_seconds, timezone_offset):
    if isinstance(time_in_seconds, str) or isinstance(time_in_seconds, unicode):
        return time_in_seconds
    ret = time.strftime('%m-%d %H:%M', time.localtime(time_in_seconds + timezone_offset))
    return ret

def get_timestamp_str(time_in_seconds, timezone_offset):
    if isinstance(time_in_seconds, str) or isinstance(time_in_seconds, unicode):
        return time_in_seconds
    ret = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(time_in_seconds + timezone_offset))
    return ret

def current_timestamp_str():
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(time.time()))

def current_timestamp_int():
    return time.time()

def get_timestamp_str_hms(time_in_seconds, timezone_offset):
    if isinstance(time_in_seconds, str) or isinstance(time_in_seconds, unicode):
        return time_in_seconds
    ret = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_in_seconds + timezone_offset))
    return ret

def reciprocal(value):
    value = float(value)
    if value != 0:
        value =  round(1.0 / value, 9)
    return value

