import time

if __name__ == '__main__':
    time_millisecond = time.time() * 1e3 - 8 * 3600 * 1000
    time_millisecond_hour_minus_1 = time_millisecond - time_millisecond % 60000 - 3600000

    print 'current: %d' % time_millisecond
    print '-1 hour: %d' % time_millisecond_hour_minus_1

