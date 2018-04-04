import logging
import threading

from collectors.collector_factory import collector_factory

TIMEOUT_COLLECT_IN_SECONDS = 60

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    global factory
    global settings

    settings = { 
        'logger': logger, 
        'timezone_offset': -8 * 3600,
        'influxdb': {
            'host': '127.0.0.1',
            'port': 8086,
            'username': 'root',
            'password': 'root',
            'database': 'market_index'
        },
        'symbols': [ 'btcusdt', 'eosbtc', 'ethbtc' ]
    }
    factory = collector_factory(settings)
    threads = []

    collectors = factory.get_all_collectors()
    for collector in collectors:
        threads.append(threading.Thread(target=collector.collect))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(TIMEOUT_COLLECT_IN_SECONDS)

    for collector in collectors:
        collector.dispose()

