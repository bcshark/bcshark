import requests

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_binance(collector):
    DEFAULT_PERIOD = "1m"
    DEFAULT_SIZE = 200

    def __init__(this, settings, market_settings):
        super(collector_binance, this).__init__(settings, market_settings)

        this.period = this.DEFAULT_PERIOD
        this.symbols_binance = this.symbols['default']

    def translate(this, objs):
        ticks = []
        for obj in objs:
            tick = market_tick()
            tick.time = obj[6] / 1000
            tick.timezone_offset = this.timezone_offset
            tick.open = obj[1]
            tick.close = obj[4]
            tick.low = obj[3]
            tick.high = obj[2]
            tick.amount = 0
            tick.volume = obj[5]
            tick.count = 0
            tick.period = this.get_generic_period_name(this.period)

            ticks.append(tick)

        return ticks

    def collect_ws(this):
        pass

    def collect_rest(this):
        timestamp = current_timestamp_str() 

        for symbol in this.symbols_binance:
            url = "klines?symbol=%s&interval=%s&limit=%d" % (symbol.upper(), this.DEFAULT_PERIOD, this.DEFAULT_SIZE)
            url = this.REST_URL + url
            data = this.http_request_json(url, None)
        
            if not data or not isinstance(data, list):
                this.logger.error('cannot get response from binance (%s)' % symbol)
                continue

            ticks = this.translate(data)
            this.bulk_save_ticks('binance', symbol, ticks)

            this.logger.info('get response from binance')

    def get_generic_period_name(this, period_name):
        if period_name == '1m':
            return '1min'

        return period_name
