import requests

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_binance(collector):
    DEFAULT_PERIOD = "1m"
    DEFAULT_SIZE = 5

    @property
    def market_name(this):
        return "binance"

    def __init__(this, settings, market_settings):
        super(collector_binance, this).__init__(settings, market_settings)

        this.period = this.DEFAULT_PERIOD

    def translate(this, obj):
        tick = market_tick()

        tick.time = long(obj[0] / 1000)
        tick.timezone_offset = this.timezone_offset
        tick.open = float(obj[1])
        tick.close = float(obj[4])
        tick.low = float(obj[3])
        tick.high = float(obj[2])
        tick.amount = float(obj[7])
        tick.volume = float(obj[5])
        tick.count = float(0)
        tick.period = this.get_generic_period_name(this.period)

        return tick

    def collect_ws(this):
        pass

    def collect_rest(this):
        for symbol in this.symbols_market:
            if symbol == "":
                continue

            url = "klines?symbol=%s&interval=%s&limit=%d" % (symbol.upper(), this.DEFAULT_PERIOD, this.DEFAULT_SIZE)
            url = this.REST_URL + url
            ticks = this.http_request_json(url, None)

            if not ticks or not isinstance(ticks, list):
                this.logger.error('cannot get ticks from binance (%s)' % symbol)
                continue

            this.bulk_save_ticks(this.get_generic_symbol_name(symbol), [ this.translate(tick) for tick in ticks ])

            this.logger.info('get ticks from binance')

    def get_generic_period_name(this, period_name):
        if period_name == '1m':
            return '1min'

        return period_name
