import requests
import json
import StringIO
import gzip
import re

from model.market_tick import  market_tick
from .collector import collector
from lib.utility import *

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
        tick = market_tick()

        tick.time = long(obj[0] / 1000)
        tick.timezone_offset = this.timezone_offset
        tick.open = float(obj[1])
        tick.close = float(obj[2])
        tick.low = float(obj[4])
        tick.high = float(obj[3])
        tick.amount = 0.0
        tick.volume = float(obj[5])
        tick.count = 0
        tick.period = this.get_generic_period_name(this.period)

        return tick

    def on_open(this, websocket_client, name):
        this.logger.info('bitfinex websocket connection established')

    def on_message(this, websocket_client, raw_message, name):
        #this.logger.info('receive message from bitfinex websocket: %s', raw_message)
        this.logger.info('receive message from bitfinex websocket: (hide)')
        
        message_json = json.loads(raw_message)

        if isinstance(message_json, dict):
            if message_json.has_key('platform'):
                status = message_json['platform']['status']

                if status == 1:
                    for symbol in this.symbols_market:
                        if symbol == '':
                            continue

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

    def collect_ws(this, name = '*'):
        if name == '*' or name == 'default':
            this.start_listen_websocket(this.WS_URL, this, 'default')

    def collect_rest(this):
        this.logger.info('bitfinex rest api by pass!')

    def get_generic_period_name(this, period_name):
        return period_name
