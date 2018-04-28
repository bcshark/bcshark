import requests

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_okex(collector):
    DEFAULT_PERIOD = "1min"
    DEFAULT_SIZE = 200
    DEFAULT_TYPE = "this_week"

    @property
    def market_name(this):
        return "okex"

    def __init__(this, settings, market_settings):
        super(collector_okex, this).__init__(settings, market_settings)
        this.period = this.DEFAULT_PERIOD

    def translate(this, obj):
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
        tick.period = this.get_generic_period_name()

        return tick

    def collect_rest(this):

        for symbol in this.symbols_market:

            if symbol == "":
                continue

            url = "future_kline.do?symbol=%s&type=%s&contract_type=%s&size=%s" % (symbol, this.DEFAULT_PERIOD, this.DEFAULT_TYPE, this.DEFAULT_SIZE)
            url = this.REST_URL + url
            ticks = this.http_request_json(url, None)

            if not ticks or not isinstance(ticks, list):
                this.logger.error('cannot get response from okex (%s)' % symbol)
                continue

            # this.bulk_save_ticks('okex', this.get_generic_symbol_name(symbol_index), ticks)
            this.bulk_save_ticks(this.get_generic_symbol_name(symbol), [ this.translate(tick) for tick in ticks ])

            this.logger.info('get response from okex')

    def get_generic_period_name(this):

        return this.DEFAULT_PERIOD
