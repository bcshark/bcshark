import requests

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_okcoin(collector):
    API_URL = "https://www.okcoin.com/api/v1/%s"
    DEFAULT_PERIOD = "1min"
    DEFAULT_SIZE = 200

    def __init__(this, settings):
        super(collector_okcoin, this).__init__(settings)
        this.period = this.DEFAULT_PERIOD
        this.symbols_okcoin = this.symbols['okcoin']

    def translate(this, objs):
        ticks = []
        for obj in objs:
            tick = market_tick()
            tick.timezone_offset = this.timezone_offset
            tick.time = obj[0] / 1000
            tick.open = obj[1]
            tick.high = obj[2]
            tick.low = obj[3]
            tick.close = obj[4]
            tick.volume = obj[5]
            tick.amount = 0
            tick.count = 0
            tick.period = this.get_generic_period_name(this.period)

            ticks.append(tick)

        return ticks

    def collect(this):
        symbol_index = -1

        time_second = int(time.time())
        time_second = time_second - time_second % 60 - 300 ## possible to miss data each 5 mins ?
        for symbol in this.symbols_okcoin:
            symbol_index += 1

            if symbol == "":
                continue

            url = "kline.do?symbol=%s&type=%s&size=%d&since=%d" % (symbol, this.DEFAULT_PERIOD, this.DEFAULT_SIZE, time_second)
            url = this.API_URL % url
            data = this.http_request_json(url, None)

            if not data or not isinstance(data, list):
                this.logger.error('cannot get response from okcoin (%s)' % symbol)
                continue

            ticks = this.translate(data)
            this.bulk_save_ticks('okcoin', this.get_generic_symbol_name(symbol_index), ticks)

            this.logger.info('get response from okcoin')

    def get_generic_symbol_name(this, symbol_index):
        symbols_default = this.symbols['default']

        return symbols_default[symbol_index]

    def get_generic_period_name(this, period_name):
        return this.DEFAULT_PERIOD
