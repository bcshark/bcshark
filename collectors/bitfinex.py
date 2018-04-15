import requests
import json
import StringIO
import gzip

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_bitfinex(collector):
    DEFAULT_PERIOD = "1min"
    DEFAULT_SIZE = 200
    DEFAULT_DELAY = 3

    def __init__(this, settings, market_settings):
        super(collector_bitfinex, this).__init__(settings, market_settings)

        this.period = this.DEFAULT_PERIOD
        this.chanId = '';

    def translate(this, ts, obj):
        ticks = []
        tick = market_tick()
        tick.time = obj[0]
        tick.timezone_offset = this.timezone_offset
        tick.open = obj[1]
        tick.close = obj[2]
        tick.low = obj[4]
        tick.high = obj[3]
        tick.amount = 0
        tick.volume = obj[5]
        tick.count = 0
        tick.period = this.get_generic_period_name(this.period)

        ticks.append(tick)

        return ticks

    def on_open(this, websocket_client):
        this.logger.info('bitfinex websocket connection established')

        this.subscription_delay = this.DEFAULT_DELAY

    def on_message(this, websocket_client, raw_message):

        this.logger.info('receive message from bitfinex websocket: %s', raw_message)
        message_json = json.loads(raw_message)
        if isinstance(message_json, dict):
            if message_json.has_key('platform'):
                status = message_json['platform']['status']
                if status == 1:
                    this.logger.info('bitfinex ws connected!')
                    subscription_msg = {
                        "event": "subscribe",
                        "channel": "candles",
                        "key": "trade:1m:tBTCUSD"
                    }
                    this.send_ws_message_json(subscription_msg)
            elif message_json.has_key('event'):
                event = message_json['event']
                this.logger.info('event=%s',event)
                if event == "subscribed":
                    this.chanId = message_json['chanId']
                    this.logger.info('bitfinex ws channel: %s', message_json['channel'])
                    this.logger.info('bitfinex ws chanId: %s', this.chanId)
                    this.logger.info('bitfinex ws key: %s', message_json['key'])
        elif isinstance(message_json, list):
            if message_json[0] == this.chanId:
                data = message_json[1]
                if isinstance(data[0],list):
                    this.logger.info('bitfinex ws ignore history long candles for chanId: %s', this.chanId)
                    return
                if data != 'hb':
                    ticks = this.translate(data[0], data)
                    this.save_tick('bitfinex_ticks', 'bitfinex', 'btcusdt', ticks[0])

    def collect_ws(this):
        this.start_listen_websocket(this.WS_URL, this)

    def collect_rest(this):
        this.logger.info('bitfinex rest api by pass!')

    def get_generic_period_name(this, period_name):
        return period_name
