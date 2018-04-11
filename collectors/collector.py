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
        sql = "select max(time), market, symbol from market_ticks where market = '%s' and symbol = '%s' group by market, symbol" % (market_name, symbol_name)
        ret = this.db_adapter.query(sql)
        ret = ret['series'][0]['values'][0][1]

        ticks = filter(lambda x: x.time + x.timezone_offset > ret, ticks)
        this.db_adapter.bulk_save_ticks(market_name, symbol_name, ticks)

