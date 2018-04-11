import requests
import datetime

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_gdax(collector):
    API_URL = "https://api.gdax.com/%s"
    DEFAULT_SIZE = 60
    DEFAULT_PERIOD = "1min"

    def __init__(this, settings):
        super(collector_gdax, this).__init__(settings)
        this.period = this.DEFAULT_PERIOD
        this.symbols_gdax = this.symbols['gdax']

    def translate(this, objs):
        ticks = []

        for obj in objs:
            tick = market_tick()
            tick.timezone_offset = this.timezone_offset
            tick.time = objs[0][0]
            tick.open = objs[0][3]
            tick.high = objs[0][2]
            tick.low = objs[0][1]
            tick.close = objs[0][4]
            tick.volume = objs[0][5]
            tick.amount = 0
            tick.count = 0
            tick.period = this.period

            ticks.append(tick)

        return ticks

    def collect(this):
        symbol_index = -1

        start = (datetime.datetime.utcnow()-datetime.timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%MZ')
        end = (datetime.datetime.utcnow()).strftime('%Y-%m-%dT%H:%MZ')

        for symbol in this.symbols_gdax:
            symbol_index += 1

            if symbol == "":
                continue

            url = "products/%s/candles?start=%s&end=%s&granularity=%s" % (symbol, start, end, this.DEFAULT_SIZE)
            url = this.API_URL % url
            data = this.http_request_json(url, None)

            if not data or not isinstance(data, list):
                this.logger.error('cannot get response from gdax (%s)' % symbol)
                continue

            ticks = this.translate(data)
            this.bulk_save_ticks('gdax', this.get_generic_symbol_name(symbol_index), ticks)

            this.logger.info('get response from gdax')

    def get_generic_symbol_name(this, symbol_index):
        symbols_default = this.symbols['default']

        return symbols_default[symbol_index]

