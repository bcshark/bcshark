import logging
import logging.handlers
import threading
import signal
import time

from lib.config import ConfigurationManager
from collectors.collector_factory import collector_factory
from adapters.influxdb_adapter import influxdb_adapter
from adapters.mysqldb_adapter import mysqldb_adapter

TIMEOUT_COLLECT_IN_SECONDS = 10

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

file_logger_handler = logging.handlers.TimedRotatingFileHandler('logs/collect.log', when = 'D', interval = 1, backupCount = 10)
file_logger_handler.suffix = "%Y-%m-%d_%H-%M-%S.log"
file_logger_handler.setFormatter(formatter)
file_logger_handler.setLevel(logging.DEBUG)
logger.addHandler(file_logger_handler)

def close_and_exit(signum, frame):
    global threads
    global settings

    logger.info('waiting for exit...3')
    for thread in threads:
        thread.join(TIMEOUT_COLLECT_IN_SECONDS)

    logger.info('waiting for exit...2')
    settings['db_adapter'].close()

    logger.info('waiting for exit...1')
    exit()

if __name__ == '__main__':
    global factory
    global threads
    global settings

    settings = ConfigurationManager('config/global.json')
    settings['logger'] = logger
    settings['db_adapter'] = influxdb_adapter(settings['influxdb'])
    #settings['db_adapter'] = mysqldb_adapter(settings['mysqldb'])

    factory = collector_factory(settings)
    threads = []
    signal.signal(signal.SIGINT, close_and_exit)

    settings['db_adapter'].open()

    collectors = factory.get_all_ws_collectors()
    for collector in collectors:
        threads.append(threading.Thread(target=collector.collect_ws))
    
    for thread in threads:
        thread.setDaemon(True) 
        thread.start()

    signal.pause()
