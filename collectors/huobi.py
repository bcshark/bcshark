import requests

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_huobi(collector):
    API_URL = "https://api.huobi.pro/market/history/%s"
    API_KEY = "077f9ced-5c9fca6ae-097127d0-95973"

    DEFAULT_PERIOD = "1day"
    DEFAULT_SIZE = 200

    def __init__(this, settings):
        super(collector_huobi, this).__init__(settings)

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

            ticks.append(tick)

        return ticks

    def collect(this):
        timestamp = current_timestamp_str() 

        for symbol in this.symbols:
            url = "kline?AccessKeyId=%s&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp=%s&peroid=%s&size=%s&symbol=%s" % (this.API_KEY, timestamp, this.DEFAULT_PERIOD, this.DEFAULT_SIZE, symbol)
            url = this.API_URL % url
            data = this.http_request_json(url, None)
        
            if not data:
                this.logger.error('cannot get response from huobi (%s)' % symbol)
                continue

            ticks = this.translate(data['data'])
            this.bulk_save_ticks('huobi', symbol, ticks)

            this.logger.info('get response from huobi')

