import requests

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_gateio(collector):
    DEFAULT_PERIOD = "1"
    DEFAULT_GROUP = "60"
    DEFAULT_TYPE = "this_week"

    @property
    def market_name(this):
        return "gateio"

    def __init__(this, settings, market_settings):
        super(collector_gateio, this).__init__(settings, market_settings)
        this.period = this.DEFAULT_PERIOD

    def translate(this, obj):
        tick = market_tick()
        tick.time = long(obj[0])/1000
        tick.volume = float(obj[1])
        tick.close = float(obj[2])
        tick.high = float(obj[3])
        tick.low = float(obj[4])
        tick.open = float(obj[5])
        tick.amount = 0.0
        tick.count = 0.0
        tick.timezone_offset = this.timezone_offset
        tick.period = this.get_generic_period_name()
        return tick

    def collect_rest(this):
        for symbol in this.symbols_market:
            if symbol == "":
                continue

            url = "candlestick2/%s?group_sec=%s&range_hour=%s" % (symbol, this.DEFAULT_GROUP, this.DEFAULT_PERIOD)
            url = this.REST_URL + url
            headers = { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36" }
            ticks = this.http_request_json(url, headers)

            if not ticks or not isinstance(ticks, dict):
                this.logger.error('cannot get response from gateio (%s)' % symbol)
                continue
            this.bulk_save_ticks(this.get_generic_symbol_name(symbol), [ this.translate(tick) for tick in ticks["data"] ])

            this.logger.info('get response from gateio')

    def get_generic_period_name(this):
        return "1min"
