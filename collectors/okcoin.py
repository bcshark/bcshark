import requests

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_okcoin(collector):
    DEFAULT_PERIOD = "1min"
    DEFAULT_SIZE = 1

    @property
    def market_name(this):
        return "okcoin"

    def __init__(this, settings, market_settings):
        super(collector_okcoin, this).__init__(settings, market_settings)
        this.period = this.DEFAULT_PERIOD

    def translate(this, obj):
        tick = market_tick()

        tick.time = long(obj[0] / 1000)
        tick.timezone_offset = this.timezone_offset
        tick.open = float(obj[1])
        tick.high = float(obj[2])
        tick.low = float(obj[3])
        tick.close = float(obj[4])
        tick.volume = float(obj[5])
        tick.amount = 0.0
        tick.count = 0.0
        tick.period = this.get_generic_period_name(this.period)

        return tick

    def collect_rest(this):
        symbol_index = -1

        time_second = int(time.time())
        time_second = time_second - time_second % 60 - 300 ## possible to miss data each 5 mins ?
        for symbol in this.symbols_market:
            if symbol == '':
                continue

            url = "kline.do?symbol=%s&type=%s&size=%d&since=%d" % (symbol, this.DEFAULT_PERIOD, this.DEFAULT_SIZE, time_second)
            url = this.REST_URL + url
            ticks = this.http_request_json(url, None)

            if not ticks or not isinstance(ticks, list):
                this.logger.error('cannot get response from okcoin (%s)' % symbol)
                continue

            this.bulk_save_ticks(this.get_generic_symbol_name(symbol), [ this.translate(tick) for tick in ticks ])

            this.logger.info('get response from okcoin')

    def get_generic_period_name(this, period_name):
        return this.DEFAULT_PERIOD
