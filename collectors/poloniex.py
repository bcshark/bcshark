import requests

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_poloniex(collector):
    API_URL = "https://poloniex.com/%s"
    DEFAULT_PERIOD = "300"

    def __init__(this, settings):
        super(collector_poloniex, this).__init__(settings)
        this.period = this.DEFAULT_PERIOD

    def translate(this, objs):
        print objs

        ticks = []
        tick = market_tick()
        tick.time = objs[0]["date"]
        tick.timezone_offset = this.timezone_offset
        tick.open = objs[0]["open"]
        tick.high = objs[0]["high"]
        tick.low = objs[0]["low"]
        tick.close = objs[0]["close"]
        tick.volume = objs[0]["volume"]
        tick.amount = 0
        tick.count = 0
        tick.period = this.get_generic_period_name(this.period)

        ticks.append(tick)

        return ticks

    def collect(this):

        time_second = int(time.time())
        time_second = time_second - time_second % 100 -300 ## possible to miss data each 5 mins ?
        for symbol in this.symbols_poloniex:
            url = "public?command=returnChartData&currencyPair=%s&start=%s&period=%s" % (symbol, time_second, this.DEFAULT_PERIOD)
            url = this.API_URL % url
            data = this.http_request_json(url, None)

            if not data:
                this.logger.error('cannot get response from poloniex (%s)' % symbol)
                continue

            ticks = this.translate(data)
            if ticks[0].time == 0:
                this.logger.info('empty data for poloniex, ignore it')
                return
            this.bulk_save_ticks('poloniex', symbol, ticks)
            this.logger.info('get response from poloniex')

    def get_generic_period_name(this, period_name):

        return "5min"
