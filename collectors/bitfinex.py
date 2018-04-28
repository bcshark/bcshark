import requests
import json
import StringIO
import gzip
import re

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_bitfinex(collector):
    DEFAULT_PERIOD = "1min"
    DEFAULT_SIZE = 200
    PATTERN_KEY = 'trade:1m:(t[A-Z]+)'

    @property
    def market_name(this):
        return "bitfinex"

    def __init__(this, settings, market_settings):
        super(collector_bitfinex, this).__init__(settings, market_settings)

        this.period = this.DEFAULT_PERIOD
        this.subscribed_channels = {}

    def translate(this, obj):
        tick = {
            "time": obj[0] / 1000,
            "timezone_offset" : this.timezone_offset,
            "open" : float(obj[1]),
            "close" : float(obj[2]),
            "low" : float(obj[4]),
            "high" : float(obj[3]),
            "amount" : float(0),
            "volume" : float(obj[5]),
            "count" : 0,
            "period" : this.get_generic_period_name(this.period)
        }

        return tick

    def on_open(this, websocket_client):
        this.logger.info('bitfinex websocket connection established')

    def on_message(this, websocket_client, raw_message):
        this.logger.info('receive message from bitfinex websocket: %s', raw_message)
        
        message_json = json.loads(raw_message)

        if isinstance(message_json, dict):
            if message_json.has_key('platform'):
                status = message_json['platform']['status']

                if status == 1:
                    for symbol in this.symbols_market:
                        subscription_msg = {
                            "event": "subscribe",
                            "channel": "candles",
                            "key": "trade:1m:%s" % symbol
                        }
                        this.send_ws_message_json(subscription_msg)
            elif message_json.has_key('event'):
                event = message_json['event']
                if event == "subscribed":
                    match = re.search(this.PATTERN_KEY, message_json['key'])

                    if match:
                        symbol_name = match.group(1)
                        channel_id = str(message_json['chanId'])
                        this.subscribed_channels[channel_id] = symbol_name
                        this.logger.info('symbol %s subscribed, channel %s', symbol_name, channel_id)
        elif isinstance(message_json, list):
            channel_id = str(message_json[0])
            if this.subscribed_channels.has_key(channel_id):
                data = message_json[1]
                symbol_name = this.subscribed_channels[channel_id]
                if isinstance(data[0], list):
                    this.logger.info('bitfinex ws ignore history long candles for symbol: %s', symbol_name)
                elif isinstance(data, list):
                    tick = this.translate(data)
                    this.save_tick(this.get_generic_symbol_name(symbol_name), tick)

    def collect_ws(this):
        this.start_listen_websocket(this.WS_URL, this)

    def collect_rest(this):
        this.logger.info('bitfinex rest api by pass!')

    def get_generic_period_name(this, period_name):
        return period_name
