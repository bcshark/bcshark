import logging
import logging.handlers
import threading
import signal
import time
import sys
import os
import getopt
import csv

from lib.config import ConfigurationManager
from lib.cache import cache_manager_factory
from collectors.collector_factory import collector_factory
from adapters.influxdb_adapter import influxdb_adapter
#from adapters.mysqldb_adapter import mysqldb_adapter

TIMEOUT_COLLECT_IN_SECONDS = 10

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

file_logger_handler = logging.handlers.TimedRotatingFileHandler(os.path.normpath(os.path.join(sys.path[0], 'logs/collect.log')), when = 'D', interval = 1, backupCount = 10)
file_logger_handler.suffix = "%Y-%m-%d_%H-%M-%S.log"
file_logger_handler.setFormatter(formatter)
file_logger_handler.setLevel(logging.DEBUG)
logger.addHandler(file_logger_handler)

def get_symbols_from_csv(file_path):
    symbols_dict = {}

    with open(os.path.normpath(os.path.join(sys.path[0], file_path)), 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in spamreader:
            symbols_dict[row[0]] = row[1:]

    return symbols_dict

def close_and_exit(signum, frame):
    global threads
    global settings

    logger.info('waiting for exit...3')
    for thread in threads:
        thread.join(TIMEOUT_COLLECT_IN_SECONDS)

    logger.info('waiting for exit...2')
    settings['db_adapter'].close()
    settings['cache_manager'].dispose()

    logger.info('waiting for exit...1')
    exit()

if __name__ == '__main__':
    global factory
    global threads
    global settings

    selected_markets = []

    try:
        opts, args = getopt.getopt(sys.argv[1:], "m:", [ "markets=" ])

        for opt, arg in opts:
            if opt in ("-m", "--markets"):
                selected_markets = arg.split(',')
    except getopt.GetoptError:
        selected_markets = []

    settings = ConfigurationManager(os.path.normpath(os.path.join(sys.path[0], 'config/global.json')))
    settings['logger'] = logger
    settings['validation_logger'] = logger
    settings['db_adapter'] = influxdb_adapter(settings['influxdb'])
    #settings['db_adapter'] = mysqldb_adapter(settings['mysqldb'])
    settings['cache_manager'] = cache_manager_factory.create(settings['cache'])
    settings['symbols'] = get_symbols_from_csv(settings['symbols']['path'])

    factory = collector_factory(settings)
    threads = []
    signal.signal(signal.SIGINT, close_and_exit)

    settings['db_adapter'].open()

    collectors = factory.get_all_ws_collectors()
    for collector in collectors:
        if not selected_markets or collector.market_name in selected_markets:
            threads.append(threading.Thread(target=collector.collect_ws))
    
    for thread in threads:
        thread.setDaemon(True) 
        thread.start()

    signal.pause()
