import logging
import threading

from collectors.collector_factory import collector_factory
from adapters.influxdb_adapter import influxdb_adapter
from adapters.mysqldb_adapter import mysqldb_adapter

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
        'mysqldb': {
            'host': '127.0.0.1',
            'port': 3306,
            'username': 'root',
            'password': '76f4dd9b',
            'database': 'market_index'
        },
        'kline': {
            'size': 200
        },
        'symbols': [ 'btcusdt', 'eosbtc', 'ethbtc' ]
    }
    #settings['db_adapter'] = influxdb_adapter(settings['influxdb'])
    settings['db_adapter'] = mysqldb_adapter(settings['mysqldb'])
    factory = collector_factory(settings)
    threads = []

    settings['db_adapter'].open()

    collectors = factory.get_all_collectors()
    for collector in collectors:
        threads.append(threading.Thread(target=collector.collect))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(TIMEOUT_COLLECT_IN_SECONDS)

    settings['db_adapter'].close()
