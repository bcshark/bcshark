import requests

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_poloniex(collector):
    DEFAULT_PERIOD = "300"

    @property
    def market_name(this):
        return "poloniex"

    def __init__(this, settings, market_settings):
        super(collector_poloniex, this).__init__(settings, market_settings)

        this.period = this.DEFAULT_PERIOD
        this.symbols_poloniex = this.symbols['poloniex']

    def translate(this, obj):
        tick = market_tick()
        tick.time = long(obj["date"])
        tick.timezone_offset = this.timezone_offset
        tick.open = float(format(obj["open"], '.12f'))
        tick.high = float(format(obj["high"], '.12f'))
        tick.low = float(format(obj["low"], '.12f'))
        tick.close = float(format(obj["close"], '.12f'))
        tick.volume = float(format(obj["quoteVolume"], '.12f'))
        tick.amount = float(format(obj["volume"], '.12f'))
        tick.count = 0.0
        tick.period = this.get_generic_period_name()

        return tick

    def collect_rest(this):
        time_second = int(time.time())
        time_second = time_second - time_second % 60 - 3600

        for symbol in this.symbols_market:
            if symbol == "":
                continue

            url = "public?command=returnChartData&currencyPair=%s&start=%s&period=%s" % (symbol, time_second, this.DEFAULT_PERIOD)
            url = this.REST_URL + url
            ticks = this.http_request_json(url, None)

            if not ticks or not isinstance(ticks, list):
                this.logger.error('cannot get response from poloniex (%s)' % symbol)
                continue

            # this.bulk_save_ticks('poloniex', this.get_generic_symbol_name(symbol_index), ticks)
            this.bulk_save_ticks(this.get_generic_symbol_name(symbol), [ this.translate(tick) for tick in ticks ])
            this.logger.info('get response from poloniex')

    def get_generic_period_name(this):
        return "5min"
