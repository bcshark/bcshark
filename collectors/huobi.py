import requests

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_huobi(collector):
    DEFAULT_PERIOD = "1min"
    DEFAULT_SIZE = 200

    def __init__(this, settings, market_settings):
        super(collector_huobi, this).__init__(settings, market_settings)

        this.period = this.DEFAULT_PERIOD
        this.symbols_huobi = this.symbols['default']

    def translate(this, objs):
        ticks = []
        for obj in objs:
            tick = market_tick()
            tick.time = obj['id']
            tick.timezone_offset = this.timezone_offset
            tick.open = obj['open']
            tick.close = obj['close']
            tick.low = obj['low']
            tick.high = obj['high']
            tick.amount = obj['amount']
            tick.volume = obj['vol']
            tick.count = obj['count']
            tick.period = this.get_generic_period_name(this.period)

            ticks.append(tick)

        return ticks

    def collect_ws(this):
        print 'huobi collect ws'

    def collect_rest(this):
        timestamp = current_timestamp_str() 

        for symbol in this.symbols_huobi:
            url = "kline?Timestamp=%s&peroid=%s&size=%s&symbol=%s" % (timestamp, this.DEFAULT_PERIOD, this.DEFAULT_SIZE, symbol)
            url = this.REST_URL + url
            data = this.http_request_json(url, None)
        
            if not data or not data.has_key('data'):
                this.logger.error('cannot get response from huobi (%s)' % symbol)
                continue

            ticks = this.translate(data['data'])
            this.bulk_save_ticks('huobi', symbol, ticks)

            this.logger.info('get response from huobi')

    def get_generic_period_name(this, period_name):
        return period_name
