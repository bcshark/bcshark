import requests
import json
import re

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_bitstamp(collector):
    DEFAULT_PERIOD = "1min"
    DEFAULT_SIZE = 200
    PATTERN_LIVE_TRADES = "live_trades([_a-zA-Z]*)"

    @property
    def market_name(this):
        return "bitstamp"

    def __init__(this, settings, market_settings):
        super(collector_bitstamp, this).__init__(settings, market_settings)

        this.period = this.DEFAULT_PERIOD

    def translate_trade(this, ts, obj):
        ret = dict(obj)

        ret['time'] = ts
        ret['timezone_offset'] = this.timezone_offset

        return ret

    def on_open(this, websocket_client):
        this.logger.info('bitstamp websocket connection established')

    def on_message(this, websocket_client, raw_message):
        this.logger.info('receive message from bitstamp websocket: %s', raw_message)
        message_json = json.loads(raw_message)

        event = message_json['event']
        data = json.loads(message_json['data'])

        if event == 'pusher:connection_established':
            this.socket_id = data['socket_id']

            for symbol in this.symbols_market:
                if symbol == '':
                    continue

                if symbol == 'btcusd':
                    channel_name = "live_trades"
                else:
                    channel_name = "live_trades_%s" % symbol

                subscription_msg = { "event": "pusher:subscribe", "data": { "channel": channel_name } }
                this.send_ws_message_json(subscription_msg)
        elif event == 'trade':
            channel = message_json['channel']
            match = re.search(this.PATTERN_LIVE_TRADES, channel)

            if match:
                symbol_name = match.group(1)[1:]
            else:
                symbol_name = 'btcusd'

            trade = this.translate_trade(long(data['timestamp']), data)
            this.save_trade(this.get_generic_symbol_name(symbol_name), trade)

    def collect_ws(this):
        this.start_listen_websocket(this.WS_URL, this)
