import os
import sys
import logging
import logging.handlers
import threading
import getopt
import csv

from lib.config import ConfigurationManager
from lib.cache import cache_manager_factory
from collectors.collector_factory import collector_factory
from adapters.influxdb_adapter import influxdb_adapter
#from adapters.mysqldb_adapter import mysqldb_adapter

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)
k30_logger = logging.getLogger('k30_logger')
validation_logger = logging.getLogger('validation_logger')

file_logger_handler = logging.handlers.TimedRotatingFileHandler(os.path.normpath(os.path.join(sys.path[0], 'logs/collect.log')), when = 'D', interval = 1, backupCount = 10)
file_logger_handler.suffix = "%Y-%m-%d_%H-%M-%S.log"
file_logger_handler.setFormatter(formatter)
file_logger_handler.setLevel(logging.DEBUG)
logger.addHandler(file_logger_handler)

file_logger_handler_k30 = logging.handlers.TimedRotatingFileHandler(os.path.normpath(os.path.join(sys.path[0], 'logs/k30_collect.log')), when = 'D', interval = 1, backupCount = 10)
file_logger_handler_k30.suffix = "%Y-%m-%d_%H-%M-%S.log"
file_logger_handler_k30.setFormatter(formatter)
file_logger_handler_k30.setLevel(logging.DEBUG)
k30_logger.addHandler(file_logger_handler_k30)

file_logger_handler_validation = logging.handlers.TimedRotatingFileHandler(os.path.normpath(os.path.join(sys.path[0], 'logs/validation.log')), when = 'D', interval = 1, backupCount = 10)
file_logger_handler_validation.suffix = "%Y-%m-%d_%H-%M-%S.log"
file_logger_handler_validation.setFormatter(formatter)
file_logger_handler_validation.setLevel(logging.DEBUG)
validation_logger.addHandler(file_logger_handler_validation)

def get_symbols_from_csv(file_path):
    symbols_dict = {}

    with open(os.path.normpath(os.path.join(sys.path[0], file_path)), 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in spamreader:
            symbols_dict[row[0]] = row[1:]

    return symbols_dict

if __name__ == '__main__':
    global factory

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
    settings['k30_logger'] = k30_logger
    settings['validation_logger'] = validation_logger
    settings['db_adapter'] = influxdb_adapter(settings['influxdb'])
    #settings['db_adapter'] = mysqldb_adapter(settings['mysqldb'])
    settings['cache_manager'] = cache_manager_factory.create(settings['cache'])
    settings['symbols'] = get_symbols_from_csv(settings['symbols']['path'])

    rest_api_timeout = settings['rest_api_timeout']

    factory = collector_factory(settings)
    threads = []

    settings['db_adapter'].open()

    collectors = factory.get_all_rest_collectors()
    for collector in collectors:
        if not selected_markets or collector.market_name in selected_markets:
            threads.append(threading.Thread(target=collector.collect_rest))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(rest_api_timeout)

    settings['db_adapter'].close()
    settings['cache_manager'].dispose()

