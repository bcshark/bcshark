import requests
import json
import StringIO
import gzip
import re

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_huobi(collector):
    DEFAULT_PERIOD = "1min"
    DEFAULT_SIZE = 1
    PATTERN_TICK = "market.([a-zA-Z]+).kline.1min"

    @property
    def market_name(this):
        return "huobi"

    def __init__(this, settings, market_settings):
        super(collector_huobi, this).__init__(settings, market_settings)

        this.period = this.DEFAULT_PERIOD

    def translate(this, ts, obj):
        tick = market_tick()

        tick.time = long(obj['id'])
        tick.timezone_offset = this.timezone_offset
        tick.open = float(obj['open'])
        tick.close = float(obj['close'])
        tick.low = float(obj['low'])
        tick.high = float(obj['high'])
        tick.amount = float(obj['amount'])
        tick.volume = float(obj['vol'])
        tick.count = float(obj['count'])
        tick.period = this.get_generic_period_name(this.period)

        return tick

    def on_open(this, websocket_client):
        this.logger.info('huobi websocket connection established')

        this.is_subscription_sent = False

    def on_message(this, websocket_client, raw_message):
        with gzip.GzipFile(fileobj = StringIO.StringIO(raw_message), mode = 'rb') as f:
            message = f.read()

        this.logger.info('receive message from huobi websocket: %s', message)
        message_json = json.loads(message)

        if message_json.has_key('ping'):
            this.send_ws_message(json.dumps({ 'pong': message_json['ping'] }))

            if not this.is_subscription_sent:
                for symbol in this.symbols_market:
                    if symbol == '':
                        continue

                    subscription_msg = {
                        "sub": "market.%s.kline.1min" % symbol,
                        "id": "id1"
                    }
                    this.send_ws_message_json(subscription_msg)
                this.is_subscription_sent = True
        elif message_json.has_key('tick'):
            channel = message_json['ch']
            match = re.search(this.PATTERN_TICK, channel)

            if match:
                symbol_name = match.group(1)
                this.save_tick(this.get_generic_period_name(symbol_name), this.translate(message_json['ts'], message_json['tick']))

    def collect_ws(this):
        this.start_listen_websocket(this.WS_URL, this)

    def collect_rest(this):
        timestamp = current_timestamp_str() 

        for symbol in this.symbols_market:
            if symbol == '':
                continue

            url = "kline?Timestamp=%s&peroid=%s&size=%s&symbol=%s" % (timestamp, this.DEFAULT_PERIOD, this.DEFAULT_SIZE, symbol)
            url = this.REST_URL + url
            ticks = this.http_request_json(url, None)
        
            if not ticks or not ticks.has_key('data'):
                this.logger.error('cannot get response from huobi (%s)' % symbol)
                continue

            this.bulk_save_ticks(this.get_generic_period_name(symbol), [ this.translate(ticks['ts'], tick) for tick in ticks['data'] ])

            this.logger.info('get response from huobi')

    def get_generic_period_name(this, period_name):
        return period_name
