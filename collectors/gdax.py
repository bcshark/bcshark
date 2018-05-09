import requests
import datetime

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_gdax(collector):
    DEFAULT_SIZE = 60
    DEFAULT_PERIOD = "1min"

    @property
    def market_name(this):
        return "gdax"

    def __init__(this, settings, market_settings):
        super(collector_gdax, this).__init__(settings, market_settings)
        this.period = this.DEFAULT_PERIOD

    def translate(this, objs):
        ticks = []

        for obj in objs:
            tick = market_tick()
            tick.time = long(objs[0][0])
            tick.timezone_offset = this.timezone_offset
            tick.open = float(objs[0][3])
            tick.high = float(objs[0][2])
            tick.low = float(objs[0][1])
            tick.close = float(objs[0][4])
            tick.volume = float(objs[0][5])
            tick.amount = 0.0
            tick.count = 0.0
            tick.period = this.period

            ticks.append(tick)

        return ticks

    def collect_rest(this):
        start = (datetime.datetime.utcnow() - datetime.timedelta(minutes = 60)).strftime('%Y-%m-%dT%H:%MZ')
        end = (datetime.datetime.utcnow()).strftime('%Y-%m-%dT%H:%MZ')

        for symbol in this.symbols_market:
            if symbol == "":
                continue

            url = "products/%s/candles?start=%s&end=%s&granularity=%s" % (symbol, start, end, this.DEFAULT_SIZE)
            url = this.REST_URL + url
            data = this.http_request_json(url, None)

            if not data or not isinstance(data, list):
                this.logger.error('cannot get response from gdax (%s)' % symbol)
                continue

            ticks = this.translate(data)
            this.bulk_save_ticks(this.get_generic_symbol_name(symbol), ticks)

            this.logger.info('get response from gdax')
