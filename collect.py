import logging
import threading

from lib.config import ConfigurationManager
from collectors.collector_factory import collector_factory
from adapters.influxdb_adapter import influxdb_adapter
from adapters.mysqldb_adapter import mysqldb_adapter

TIMEOUT_COLLECT_IN_SECONDS = 60

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    global factory

    settings = ConfigurationManager('config/global.json')
    settings['logger'] = logger
    settings['db_adapter'] = influxdb_adapter(settings['influxdb'])
    #settings['db_adapter'] = mysqldb_adapter(settings['mysqldb'])

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

