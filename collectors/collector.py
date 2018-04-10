import requests

from .utility import *

class collector(object):
    DEFAULT_TIMEOUT_IN_SECONDS = 1;

    def __init__(this, settings):
        this.settings = settings
        this.logger = settings['logger']
        this.db_adapter = settings['db_adapter']
        this.symbols = settings['symbols']
        this.timezone_offset = settings['timezone_offset']

    def http_request_json(this, url, headers):
        try:
            res = requests.get(url, headers = headers, timeout = this.DEFAULT_TIMEOUT_IN_SECONDS)

            return res.json()
        except Exception, e:
            return None

    def bulk_save_ticks(this, market_name, symbol_name, ticks):
        this.db_adapter.bulk_save_ticks(market_name, symbol_name, ticks)
