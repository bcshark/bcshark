import requests

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_okex(collector):
    API_URL = "https://www.okex.com/api/v1/%s"
    DEFAULT_PERIOD = "1min"
    DEFAULT_SIZE = 1
    DEFAULT_TYPE = "this_week"

    def __init__(this, settings):
        super(collector_okex, this).__init__(settings)
        this.period = this.DEFAULT_PERIOD

    def translate(this, objs):
        ticks = []
        for obj in objs:
            tick = market_tick()
            tick.time = obj[0]
            tick.open = obj[1]
            tick.high = obj[2]
            tick.low = obj[3]
            tick.close = obj[4]
            tick.volume = obj[5]
            tick.period = this.period

            ticks.append(tick)

        return ticks

    def collect(this):

        for symbol in this.symbols:
            url = "future_kline.do?symbol=%s&type=%s&contract_type=%s&size=%s" % (symbol, this.DEFAULT_PERIOD, this.DEFAULT_TYPE, this.DEFAULT_SIZE)
            url = this.API_URL % url
            data = this.http_request_json(url, None)

            if not data:
                this.logger.error('cannot get response from okex (%s)' % symbol)
                continue

            ticks = this.translate(data)
            this.bulk_save_tick('okex', symbol, ticks)

            this.logger.info('get response from okex')

