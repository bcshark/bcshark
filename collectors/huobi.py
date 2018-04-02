import requests

from .collector import collector

class collector_huobi(collector):
    API_URL = "https://api.huobi.pro/market/history/%s"

    def __init__(this, logger):
        super(collector_huobi, this).__init__(logger)

    def collect(this):
        url = this.API_URL % "kline?AccessKeyId=077f9ced-5c9fca6ae-097127d0-95973&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp=2018-03-31T19:00:00&peroid=1day&size=200&symbol=btcusdt"
        data = this.http_request_json(url)
        
        if not data:
            this.logger.error('cannot get response from huobi')

        this.logger.info('get response from huobi')
        print data

