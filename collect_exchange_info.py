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
from adapters.mongodb_adapter import mongodb_adapter

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

file_logger_handler = logging.handlers.TimedRotatingFileHandler(os.path.normpath(os.path.join(sys.path[0], 'logs/exchangeinfo.log')), when = 'D', interval = 1, backupCount = 10)
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
    settings['db_adapter'] = mongodb_adapter(settings['mongodb'])
    settings['cache_manager'] = cache_manager_factory.create(settings['cache'])
    settings['symbols'] = get_symbols_from_csv(settings['symbols']['path'])

    rest_api_timeout = settings['rest_api_timeout']

    factory = collector_factory(settings)
    threads = []

    settings['db_adapter'].open()

    collectors = factory.get_all_rest_collectors()
    for collector in collectors:
        if not selected_markets or collector.market_name in selected_markets:
            if collector.collect_exchange_info:
                threads.append(threading.Thread(target=collector.collect_exchange_info))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(rest_api_timeout)

    settings['db_adapter'].close()
    settings['cache_manager'].dispose()

