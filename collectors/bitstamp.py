import requests
import json
import StringIO
import gzip

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_bitstamp(collector):
    DEFAULT_PERIOD = "1min"
    DEFAULT_SIZE = 200

    def __init__(this, settings, market_settings):
        super(collector_bitstamp, this).__init__(settings, market_settings)

        this.period = this.DEFAULT_PERIOD
        this.symbols_bitstamp = this.symbols['default']

    def translate(this, ts, objs):
        ticks = []
        for obj in objs:
            tick = market_tick()
            #FIXME: change this back to id
            tick.time = obj['id']
            #tick.time = ts
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

            subscription_msg = { "event": "pusher:subscribe", "data": { "channel": "live_trades" } }
            this.send_ws_message_json(subscription_msg)

            subscription_msg = { "event": "pusher:subscribe", "data": { "channel": "diff_order_book" } }
            this.send_ws_message_json(subscription_msg)
        elif event == 'trade':
            trade = this.translate_trade(long(data['timestamp']), data)
            this.save_tick('bitstamp_trades', 'bitstamp', 'btcusdt', trade)

    def collect_ws(this):
        this.start_listen_websocket(this.WS_URL, this)

    def collect_rest(this):
        pass

    def get_generic_period_name(this, period_name):
        return period_name
