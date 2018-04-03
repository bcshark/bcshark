import requests

from .collector import collector
from .utility import *

class collector_huobi(collector):
    API_URL = "https://api.huobi.pro/market/history/%s"
    API_KEY = "077f9ced-5c9fca6ae-097127d0-95973"

    DEFAULT_PERIOD = "1day"
    DEFAULT_SIZE = 200

    def __init__(this, settings):
        super(collector_huobi, this).__init__(settings)

    def collect(this):
        timestamp = current_timestamp_str() 

        for symbol in this.symbols:
            url = "kline?AccessKeyId=%s&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp=%s&peroid=%s&size=%s&symbol=%s" % (this.API_KEY, timestamp, this.DEFAULT_PERIOD, this.DEFAULT_SIZE, symbol)
            url = this.API_URL % url
            data = this.http_request_json(url)
        
            if not data:
                this.logger.error('cannot get response from huobi (%s)' % symbol)
                continue

            this.bulk_save_tick('huobi', symbol, data['data'])

            this.logger.info('get response from huobi')

